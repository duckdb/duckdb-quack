#include "quack_fetch_ahead.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Async fetch task
//===--------------------------------------------------------------------===//
// Performs the blocking FETCH POST + response decode on an ASYNC-pool thread and publishes the
// decoded batch into the read-ahead buffer under the server-assigned batch index.
// TODO: there's read ahead logic implemented in core for multi file reader, a common abstraction
// could be nice.
class QuackFetchDataTask : public AsyncTask {
public:
	QuackFetchDataTask(QuackFetchAhead &fetch_ahead_p, unique_ptr<QuackClientWrapper> client_wrapper_p)
	    : fetch_ahead(fetch_ahead_p), client_wrapper(std::move(client_wrapper_p)) {
	}

	void Execute() override {
		// Surface errors through the buffer first so blocked scan threads wake and rethrow them,
		// then let the queue capture them as well.
		try {
			ExecuteInternal();
		} catch (std::exception &ex) {
			fetch_ahead.buffer->SetError(ErrorData(ex));
			fetch_ahead.FinishTask(std::move(client_wrapper), false);
			throw;
		} catch (...) {
			fetch_ahead.buffer->SetError(ErrorData("Unknown error in quack fetch task"));
			fetch_ahead.FinishTask(std::move(client_wrapper), false);
			throw;
		}
	}

private:
	void ExecuteInternal() {
		auto &client = client_wrapper->GetClient();
		auto start_time = QuackNowMillis();
		// context=nullptr: called off the execution thread, must not touch ClientContext.
		auto response_body = client.PostRaw(nullptr, fetch_ahead.payload->GetData(), fetch_ahead.payload_size);
		auto duration_ms = QuackNowMillis() - start_time;

		auto response = QuackClient::DecodeResponse(response_body);

		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		client.LogRequest(Logger::Get(fetch_ahead.logger), MessageType::FETCH_REQUEST, fetch_ahead.connection_id,
		                  fetch_ahead.client_query_id, string(), duration_ms, response->Type(), error);

		if (response->Type() == MessageType::ERROR_RESPONSE) {
			response->Cast<ErrorResponse>().Error().Throw();
		}
		if (response->Type() != FetchResponseMessage::TYPE) {
			throw IOException("Expected fetch_response, got %s instead", MessageTypeToString(response->Type()));
		}

		auto &fetch_response = response->Cast<FetchResponseMessage>();
		auto &wrappers = fetch_response.MutableResults();
		// Recycle the client before publishing: a consumer TopUp triggered by this batch must
		// find an idle client.
		fetch_ahead.ReturnClient(std::move(client_wrapper));
		bool pushed = false;
		if (wrappers.empty()) {
			fetch_ahead.no_more_fetches = true;
		} else {
			auto batch_index = fetch_response.BatchIndex();
			if (!batch_index.IsValid()) {
				throw IOException("fetch_response with data is missing its batch index");
			}
			// Convert to owned chunks (buffers are shared, Reference is cheap) so decode stays here.
			vector<unique_ptr<DataChunk>> chunks;
			chunks.reserve(wrappers.size());
			for (auto &wrapper : wrappers) {
				auto owned = make_uniq<DataChunk>();
				owned->InitializeEmpty(wrapper->Chunk().GetTypes());
				owned->Reference(wrapper->Chunk());
				chunks.push_back(std::move(owned));
			}
			// deliver in index order: scan threads must observe monotonically increasing batch indices
			fetch_ahead.buffer->PushOrdered(std::move(chunks), batch_index.GetIndex(), 0, true, optional_idx());
			pushed = true;
		}
		fetch_ahead.FinishTask(nullptr, pushed);
	}

private:
	QuackFetchAhead &fetch_ahead;
	unique_ptr<QuackClientWrapper> client_wrapper;
};

//===--------------------------------------------------------------------===//
// QuackFetchAhead
//===--------------------------------------------------------------------===//
idx_t QuackFetchAhead::GetReadAheadDepth(ClientContext &context) {
	Value val;
	idx_t depth = 0;
	if (context.TryGetCurrentSetting("quack_fetch_read_ahead", val) && !val.IsNull()) {
		depth = val.GetValue<uint64_t>();
	}
	if (depth == 0) {
		depth = MaxValue<idx_t>(1, (idx_t)TaskScheduler::GetScheduler(context).NumberOfAsyncThreads());
	}
	return depth;
}

QuackFetchAhead::QuackFetchAhead(ClientContext &context, QuackClientConnection &connection, hugeint_t query_uuid,
                                 idx_t depth_p)
    : depth(MaxValue<idx_t>(depth_p, 1)) {
	queue = make_uniq<ManagedAsyncTaskQueue>(context, depth);
	if (!queue->IsAsync()) {
		// Register runs the POST inline on the scan thread; deeper pipelining is impossible.
		depth = 1;
	}
	// Ordered stream: server-assigned FETCH batch indices are dense starting at 1 (the PREPARE
	// batch is 0), so seed the delivery cursor there and release batches in index order.
	buffer = make_shared_ptr<QuackDataStream>(vector<LogicalType>(), true);
	buffer->SetWatermarkAndDrain(optional_idx(1));

	FetchRequestMessage fetch_msg(connection.ConnectionId(), query_uuid);
	payload = make_uniq<MemoryStream>();
	QuackClient::EncodeRequest(context, fetch_msg, *payload);
	payload_size = payload->GetPosition();
	connection_id = fetch_msg.ConnectionId();
	client_query_id = fetch_msg.ClientQueryId();
	logger = context.logger;

	for (idx_t i = 0; i < depth; i++) {
		idle_clients.push_back(connection.GetClient(context));
	}
	TopUp();
}

QuackFetchAhead::~QuackFetchAhead() {
	StopAndDrain();
}

void QuackFetchAhead::TopUp() {
	while (!stop && !no_more_fetches) {
		auto current = outstanding.load();
		if (current >= depth) {
			break;
		}
		if (!outstanding.compare_exchange_weak(current, current + 1)) {
			continue;
		}
		unique_ptr<QuackClientWrapper> client;
		{
			lock_guard<mutex> guard(client_lock);
			if (idle_clients.empty()) {
				--outstanding;
				break;
			}
			client = std::move(idle_clients.back());
			idle_clients.pop_back();
		}
		++in_flight;
		queue->Register(make_uniq<QuackFetchDataTask>(*this, std::move(client)), payload_size);
	}
}

void QuackFetchAhead::BatchConsumed() {
	--outstanding;
	TopUp();
}

void QuackFetchAhead::ReturnClient(unique_ptr<QuackClientWrapper> client) {
	lock_guard<mutex> guard(client_lock);
	idle_clients.push_back(std::move(client));
}

void QuackFetchAhead::FinishTask(unique_ptr<QuackClientWrapper> client, bool pushed_batch) {
	if (client) {
		ReturnClient(std::move(client));
	}
	if (!pushed_batch) {
		// Nothing entered the buffer for this fetch; release its pipeline slot.
		--outstanding;
	}
	if (--in_flight == 0 && no_more_fetches) {
		buffer->Finish();
	}
}

void QuackFetchAhead::StopAndDrain() {
	stop = true;
	if (queue) {
		try {
			queue->Close();
		} catch (...) {
			// Fetch errors were already surfaced through the buffer; the scan rethrows them.
		}
		queue.reset();
	}
	lock_guard<mutex> guard(client_lock);
	idle_clients.clear();
}

} // namespace duckdb
