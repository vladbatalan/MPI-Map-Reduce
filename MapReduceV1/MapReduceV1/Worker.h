#pragma once
#include <mpi.h>
#include <string>
#include "Logger.h"

using namespace std;

class Worker {
private:
	// Coordonatorul
	const int coordonator = 0;
	
	// Comunication
	char message[100]; 
	int tag = 0;
	MPI_Status status;

	// Global parameters
	int my_rank;

	// Logger
	Logger logger;

public:
	Worker(int my_rank, Logger loger);

	void MainLoop();

	void MapLoop(string worker_dir);

	void FileMapper(string path, string worker_dir);

	void SendFinishedTaskMessage();

	void ReduceLoop(int existing_workers, string intermediar_file, string output_file);

	void FileReducer(int existing_workers, string file_name, string intermediar_file, string output_file);
};