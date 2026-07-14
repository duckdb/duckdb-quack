#include "quack_fetch_ahead.hpp"

#include "duckdb/common/enums/task_scheduler_type.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <chrono>

namespace duckdb {

//===--------------------------------------------------------------------===//
// QuackFetchBuffer
//===--------------------------------------------------------------------===//
idx_t QuackFetchBuffer::ClaimBatch() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return next_claim++;
}

void QuackFetchBuffer::PushBatch(idx_t batch_index, vector<unique_ptr<DataChunk>> chunks) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (finished || errored) {
			return;
		}
		batches[batch_index] = std::move(chunks);
	}
	cv.notify_all();
}

QuackFetchBuffer::PopStatus QuackFetchBuffer::TryPopClaimed(idx_t claim, vector<unique_ptr<DataChunk>> &chunks_out) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (errored) {
		return PopStatus::ERRORED;
	}
	auto entry = batches.find(claim);
	if (entry != batches.end()) {
		chunks_out = std::move(entry->second);
		batches.erase(entry);
		return PopStatus::BATCH;
	}
	// Indices are dense and Finish() runs after the last push, so an absent claim can never arrive.
	if (finished) {
		return PopStatus::FINISHED;
	}
	return PopStatus::EMPTY;
}

void QuackFetchBuffer::WaitForBatch(idx_t claim) {
	annotated_unique_lock<annotated_mutex> guard(lock);
	if (batches.find(claim) != batches.end() || finished || errored) {
		return;
	}
	// Bounded wait so the scan can re-check cancellation even if the batch never arrives.
	cv.wait_for(guard, std::chrono::milliseconds(200));
}

void QuackFetchBuffer::Finish() {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		finished = true;
	}
	cv.notify_all();
}

void QuackFetchBuffer::SetError(ErrorData error_p) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!errored) {
			errored = true;
			error = std::move(error_p);
		}
		finished = true;
	}
	cv.notify_all();
}

ErrorData QuackFetchBuffer::GetError() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return error;
}

//===--------------------------------------------------------------------===//
// Async fetch task
//===--------------------------------------------------------------------===//
// Runs the FETCH POST + response decode on an ASYNC-pool thread, publishing the decoded batch.
// TODO: possibly converge with core's multi-file read-ahead abstraction.
class QuackFetchDataTask : public AsyncTask {
public:
	QuackFetchDataTask(QuackFetcher &fetcher_p, unique_ptr<QuackClientWrapper> client_wrapper_p)
	    : fetcher(fetcher_p), client_wrapper(std::move(client_wrapper_p)) {
	}

	void Execute() override {
		// Surface errors through the buffer first so blocked scan threads wake and rethrow them.
		try {
			ExecuteInternal();
		} catch (std::exception &ex) {
			fetcher.buffer->SetError(ErrorData(ex));
			fetcher.FinishTask(std::move(client_wrapper), false);
			throw;
		} catch (...) {
			fetcher.buffer->SetError(ErrorData("Unknown error in quack fetch task"));
			fetcher.FinishTask(std::move(client_wrapper), false);
			throw;
		}
	}

private:
	void ExecuteInternal() {
		auto &client = client_wrapper->GetClient();
		auto start_time = QuackNowMillis();
		// context=nullptr: called off the execution thread, must not touch ClientContext.
		auto response_body = client.PostRaw(nullptr, fetcher.payload->GetData(), fetcher.payload_size);
		auto duration_ms = QuackNowMillis() - start_time;

		auto response = QuackClient::DecodeResponse(response_body);

		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		client.LogRequest(Logger::Get(fetcher.logger), MessageType::FETCH_REQUEST, fetcher.connection_id,
		                  fetcher.client_query_id, string(), duration_ms, response->Type(), error);

		if (response->Type() == MessageType::ERROR_RESPONSE) {
			response->Cast<ErrorResponse>().Error().Throw();
		}
		if (response->Type() != FetchResponseMessage::TYPE) {
			throw IOException("Expected fetch_response, got %s instead", MessageTypeToString(response->Type()));
		}

		auto &fetch_response = response->Cast<FetchResponseMessage>();
		auto &wrappers = fetch_response.MutableResults();
		// Recycle the client before publishing: a TopUp triggered by this batch needs an idle client.
		fetcher.ReturnClient(std::move(client_wrapper));
		if (fetcher.debug_delay_ms > 0) {
			// DEBUG SETTING: randomize publish order to stress ordered batch delivery
			RandomEngine random;
			ThreadUtil::SleepMs(random.NextRandomInteger(0, NumericCast<uint32_t>(fetcher.debug_delay_ms)));
		}
		bool pushed = false;
		if (wrappers.empty()) {
			fetcher.no_more_fetches = true;
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
			// the thread that claimed this index pops exactly this batch
			fetcher.buffer->PushBatch(batch_index.GetIndex(), std::move(chunks));
			pushed = true;
		}
		fetcher.FinishTask(nullptr, pushed);
	}

private:
	QuackFetcher &fetcher;
	unique_ptr<QuackClientWrapper> client_wrapper;
};

//===--------------------------------------------------------------------===//
// QuackFetcher
//===--------------------------------------------------------------------===//
idx_t QuackFetcher::GetReadAheadDepth(ClientContext &context) {
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

QuackFetcher::QuackFetcher(ClientContext &context, QuackClientConnection &connection, hugeint_t query_uuid,
                           idx_t depth_p)
    : depth(MaxValue<idx_t>(depth_p, 1)) {
	queue = make_uniq<ManagedAsyncTaskQueue>(context, depth);
	if (!queue->IsAsync()) {
		// Register runs the POST inline on the scan thread; deeper pipelining is impossible.
		depth = 1;
	}
	Value delay_val;
	if (context.TryGetCurrentSetting("quack_debug_fetch_delay_ms", delay_val) && !delay_val.IsNull()) {
		debug_delay_ms = delay_val.GetValue<uint64_t>();
	}
	// Claims start at 1: server-assigned FETCH batch indices are dense from 1 (the PREPARE batch is 0).
	buffer = make_shared_ptr<QuackFetchBuffer>();

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

QuackFetcher::~QuackFetcher() {
	StopAndDrain();
}

QuackFetchResult QuackFetcher::GetBatch(ClientContext &context, TableFunctionInput &input, optional_idx &claim,
                                        idx_t &batch_index, vector<unique_ptr<DataChunk>> &chunks) {
	if (!claim.IsValid()) {
		claim = optional_idx(buffer->ClaimBatch());
	}
	while (true) {
		switch (buffer->TryPopClaimed(claim.GetIndex(), chunks)) {
		case QuackFetchBuffer::PopStatus::BATCH:
			batch_index = claim.GetIndex();
			claim = optional_idx();
			BatchConsumed();
			return QuackFetchResult::BATCH;
		case QuackFetchBuffer::PopStatus::FINISHED:
			return QuackFetchResult::FINISHED;
		case QuackFetchBuffer::PopStatus::ERRORED:
			buffer->GetError().Throw();
			return QuackFetchResult::FINISHED;
		case QuackFetchBuffer::PopStatus::EMPTY: {
			// refill any free pipeline slots before parking
			TopUp();
			vector<unique_ptr<AsyncTask>> tasks;
			tasks.push_back(make_uniq<QuackWaitForBatchTask>(buffer, claim.GetIndex()));
			AsyncResult async_result(std::move(tasks), TaskSchedulerType::ASYNC);
			if (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
				// yield BLOCKED; the wait task reschedules this scan once a batch is ready
				input.async_result = std::move(async_result);
				return QuackFetchResult::BLOCKED;
			}
			// Synchronous fallback (async_threads=0): run the wait inline, then retry.
			async_result.ExecuteTasksSynchronously();
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			continue;
		}
		}
	}
}

void QuackFetcher::TopUp() {
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

void QuackFetcher::BatchConsumed() {
	--outstanding;
	TopUp();
}

void QuackFetcher::ReturnClient(unique_ptr<QuackClientWrapper> client) {
	lock_guard<mutex> guard(client_lock);
	idle_clients.push_back(std::move(client));
}

void QuackFetcher::FinishTask(unique_ptr<QuackClientWrapper> client, bool pushed_batch) {
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

void QuackFetcher::StopAndDrain() {
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
