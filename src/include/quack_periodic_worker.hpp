#pragma once

#include <condition_variable>
#include <functional>
#include <thread>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/chrono.hpp"

namespace duckdb {

//! A background thread that runs `tick` roughly every `interval()` until stopped.
class QuackPeriodicWorker {
public:
	~QuackPeriodicWorker() {
		Stop();
	}

	void Start(std::function<milliseconds()> interval_p, std::function<void()> tick_p) {
		D_ASSERT(!thread.joinable());
		interval = std::move(interval_p);
		tick = std::move(tick_p);
#if defined(__EMSCRIPTEN__) && !defined(__EMSCRIPTEN_PTHREADS__)
		// Single-threaded wasm cannot start a thread, so the tick never runs there
		// TODO: drive the tick from the event loop instead (emscripten_set_interval)
#else
		thread = std::thread(&QuackPeriodicWorker::Loop, this);
#endif
	}

	//! Wake the worker without joining it. Safe to call from the tick itself.
	void RequestStop() {
		{
			std::lock_guard<std::mutex> guard(lock);
			stopping = true;
		}
		cv.notify_one();
	}

	//! Wake the worker and join its thread
	void Stop() {
		RequestStop();
		if (thread.joinable()) {
			thread.join();
		}
	}

private:
	void Loop() {
		std::unique_lock<std::mutex> guard(lock);
		while (!stopping) {
			if (cv.wait_for(guard, interval(), [&] { return stopping; })) {
				break;
			}
			guard.unlock();
			try {
				tick();
			} catch (...) {
			}
			guard.lock();
		}
	}

	std::function<milliseconds()> interval;
	std::function<void()> tick;
	std::thread thread;
	std::mutex lock;
	std::condition_variable cv;
	bool stopping = false;
};

} // namespace duckdb
