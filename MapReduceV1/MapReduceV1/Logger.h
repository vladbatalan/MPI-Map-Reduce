#pragma once
#include <string>
#include <thread>
#include <mutex>

using namespace std;

class Logger {
private:
	string dir;
	const string log_file = "logs.txt";
	mutex m;

public:
	Logger(string dir);
	Logger(Logger& logger);
	void Log(string message);
	void Clear();
};