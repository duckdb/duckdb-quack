#include "quack_fetch_ahead.hpp"

#include "duckdb/common/enums/task_scheduler_type.hpp"
#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <chrono>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Async fetch task
//===--------------------------------------------------------------------===//
// Runs the FETCH POST + response decode on an ASYNC-pool thread, publishing the decoded batch.
// TODO: possibly converge with core's multi-file read-ahead abstraction.
class QuackFetchDataTask : public AsyncTask {
public:
	QuackFetchDataTask(QuackFetcher &fetcher_p, unique_ptr<QuackClientWrapper> client_wrapper_p, idx_t request_index_p,
	                   unique_ptr<MemoryStream> payload_p, idx_t payload_size_p)
	    : fetcher(fetcher_p), client_wrapper(std::move(client_wrapper_p)), request_index(request_index_p),
	      payload(std::move(payload_p)), payload_size(payload_size_p) {
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
		// This task's payload names its batch index, so an HTTP-level retry of this POST asks the
		// server for the SAME batch again instead of popping the next one.
		auto response_body = client.PostRaw(nullptr, payload->GetData(), payload_size);
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
			// terminal response: this request's index lies beyond the stream's total
			auto total = fetch_response.TotalBatches();
			if (total.IsValid()) {
				fetcher.expected_total = total.GetIndex();
			}
			fetcher.no_more_fetches = true;
		} else {
			auto batch_index = fetch_response.BatchIndex();
			if (!batch_index.IsValid()) {
				throw IOException("fetch_response with data is missing its batch index");
			}
			if (batch_index.GetIndex() != request_index) {
				throw IOException("fetch_response carries batch %llu but batch %llu was requested",
				                  batch_index.GetIndex(), request_index);
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
			fetcher.buffer->PushBatch(request_index, std::move(chunks));
			fetcher.RecordReceived(request_index);
			pushed = true;
		}
		fetcher.FinishTask(nullptr, pushed);
	}

private:
	QuackFetcher &fetcher;
	unique_ptr<QuackClientWrapper> client_wrapper;
	//! The client-visible batch index this task requests; fixed at registration.
	idx_t request_index;
	//! This task's encoded FETCH request (index + ack baked in); identical across HTTP retries.
	unique_ptr<MemoryStream> payload;
	idx_t payload_size;
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

	this->query_uuid = query_uuid;
	connection_id = connection.ConnectionId();
	logger = context.logger;
	// Captured once: async tasks read it concurrently for request logging, and it is per-query state.
	if (context.transaction.HasActiveTransaction()) {
		auto raw_query_id = context.transaction.GetActiveQuery();
		if (raw_query_id != DConstants::INVALID_INDEX) {
			client_query_id = raw_query_id;
		}
	}

	for (idx_t i = 0; i < depth; i++) {
		idle_clients.push_back(connection.GetClient(context));
	}
	TopUp(context);
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
		case QuackClaimPopStatus::BATCH:
			batch_index = claim.GetIndex();
			claim = optional_idx();
			BatchConsumed(context);
			return QuackFetchResult::BATCH;
		case QuackClaimPopStatus::FINISHED:
			return QuackFetchResult::FINISHED;
		case QuackClaimPopStatus::ERRORED:
			buffer->GetError().Throw();
			return QuackFetchResult::FINISHED;
		case QuackClaimPopStatus::EMPTY: {
			// refill any free pipeline slots before parking
			TopUp(context);
			if (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR && input.interrupt_state) {
				auto completion = buffer->RegisterWaiter(claim.GetIndex());
				if (!completion || !completion->TryPark(*input.interrupt_state)) {
					// the batch arrived (or the stream ended) meanwhile: retry the pop
					continue;
				}
				// parked; publishing this claim's batch wakes exactly this scan task
				input.async_result = AsyncResultType::BLOCKED;
				return QuackFetchResult::BLOCKED;
			}
			// Synchronous fallback (async_threads=0): bounded inline wait, then retry.
			buffer->WaitForBatch(claim.GetIndex());
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			continue;
		}
		}
	}
}

void QuackFetcher::TopUp(ClientContext &context) {
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
		// Each task requests one explicit index, assigned in dense order; its payload is encoded once
		// here (with the current ack) and never changes, so HTTP retries are idempotent.
		auto request_index = next_request++;
		FetchRequestMessage fetch_msg(connection_id, query_uuid, request_index, CurrentAck());
		auto request_payload = make_uniq<MemoryStream>();
		QuackClient::EncodeRequest(context, fetch_msg, *request_payload);
		auto request_size = request_payload->GetPosition();
		++in_flight;
		queue->Register(make_uniq<QuackFetchDataTask>(*this, std::move(client), request_index,
		                                              std::move(request_payload), request_size),
		                request_size);
	}
}

void QuackFetcher::BatchConsumed(ClientContext &context) {
	--outstanding;
	TopUp(context);
}

void QuackFetcher::RecordReceived(idx_t index) {
	lock_guard<mutex> guard(ack_lock);
	acked.Insert(index);
}

idx_t QuackFetcher::CurrentAck() {
	lock_guard<mutex> guard(ack_lock);
	return acked.ContiguousMax();
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
		// Clean end of stream: validate against the server's announced total (the buffer counted its
		// own pushes), so lost batches fail loudly. Error/cancel paths never reach here with a total.
		auto expected = expected_total.load();
		buffer->Finish(expected == DConstants::INVALID_INDEX ? optional_idx() : optional_idx(expected));
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
