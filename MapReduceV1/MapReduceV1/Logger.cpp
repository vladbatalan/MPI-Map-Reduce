#include <fstream>
#include <filesystem>
#include "Logger.h"

namespace fs = filesystem;

Logger::Logger(string dir):dir(dir)
{
	// Create dir
	fs::create_directory(dir);

	// If exists, clear file
	Clear();
}

Logger::Logger(Logger& logger)
{
	this->dir = logger.dir;
}

void Logger::Log(string message)
{
	// Must be synchronized
	m.lock();
	ofstream ofs(dir + "\\" + log_file, ios_base::app);
	ofs << message << '\n';
	ofs.close();
	m.unlock();
	// Must be synchronized
}

void Logger::Clear()
{
	ofstream ofs;
	ofs.open(dir + "\\" + log_file, std::ofstream::out | std::ofstream::trunc);
	ofs.close();
}
