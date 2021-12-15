#include <iostream>
#include <fstream>
#include <mpi.h>
#include <filesystem>
#include "CoordonatorUnit.h"

using namespace std;
namespace fs = filesystem;

CoordonatorUnit::CoordonatorUnit(int processes, string input_path, string output_path, Logger logger)
	: workers(processes - 1), input_path(input_path), output_path(output_path), logger(logger)
{

	if (PROCESS_ONLY == ONLY_MAP || PROCESS_ONLY == BOTH) {

		// Creare folder intermediar
		fs::create_directory(intermediar_path);

		// Curata folderul intermediar in caz ca acesta contine elemente
		for (const auto& entry : std::filesystem::directory_iterator(intermediar_path))
			std::filesystem::remove_all(entry.path());
	}
}

void CoordonatorUnit::BroadcastWorkersString(char* message) {

	for (int w = 1; w <= workers; w++) {

		// Trimite la worker
		MPI_Send(message, strlen(message) + 1, MPI_CHAR, w, 0, MPI_COMM_WORLD);
	}
}

void CoordonatorUnit::BroadcastWorkersInt(int value)
{
	for (int w = 1; w <= workers; w++) {

		// Trimite la worker
		MPI_Send(&value, 1, MPI_INT, w, 0, MPI_COMM_WORLD);
	}
}

void CoordonatorUnit::RoundRobinTasks(vector<string> tasks)
{

	// Trimite numarul de fisiere cate va primi fiecare worker
	int files_per_worker = tasks.size() / workers;
	int remain_files = tasks.size() % workers;

	for (int w = 1; w <= workers; w++) {

		int files_to_evaluate = files_per_worker + (w <= remain_files ? 1 : 0);

		// Trimite la worker
		MPI_Send(&files_to_evaluate, 1, MPI_INT, w, 0, MPI_COMM_WORLD);
	}

	// fiecare worker primeste de procesat numar_fisiere/workeri 
	for (unsigned file_index = 0; file_index < tasks.size(); file_index++) {

		// destination
		int worker = (file_index % workers) + 1;

		// creaza mesajul
		char* message = tasks[file_index].data();

		// vom trimite worker-ului corespondent mesaj cu numele fisierului
		MPI_Send(message, strlen(message) + 1, MPI_CHAR, worker, 0, MPI_COMM_WORLD);
	}

}

void CoordonatorUnit::WaitForWorkers()
{
	// Comunicare
	MPI_Status status;

	int worker = 1;
	// Asteapta cate un mesaj de terminare de la fiecare worker
	for (worker; worker <= workers; worker++)
	{
		// Wait for command
		MPI_Recv(message, 100, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
		string command(message);
		int unit = status.MPI_SOURCE;

		logger.Log("Coordonator[ 0 ]: Received end_task from W[ " + to_string(unit) + " ]");

	}
}

void CoordonatorUnit::MapReduce() {

	// Lista cu fisierele de verificat
	vector<string> input_files_paths;

	// Detecteaza fisierele din folderul de input
	for (const auto& entry : fs::directory_iterator(input_path)) {
		input_files_paths.push_back(entry.path().string());
	}

	if (PROCESS_ONLY == ONLY_MAP || PROCESS_ONLY == BOTH) {

		// Etapa 1: Mapare 
		// Asigneaza fisierele workerilor
		//cout << "Coordonator[ 0 ]: Initialising Map Phase ... \n";
		logger.Log("Coordonator[ 0 ]: Initialising Map Phase ...");
		MapStep(input_files_paths);
	}

	if (PROCESS_ONLY == ONLY_REDUCE || PROCESS_ONLY == BOTH) {

		// Etapa 2: Reduce
		// Va atribui fiecarui worker litere pentru a fi verificate
		logger.Log("Coordonator[ 0 ]: Initialising Reduce Phase ...");
		ReduceStep();
	}

	// Finalizare
	// Opreste workerii
	//cout << "Coordonator[ 0 ]: Terminating workers ...  \n";
	logger.Log("Coordonator[ 0 ]: Terminating workers ...");
	sprintf(message, "exit");
	BroadcastWorkersString(message);
}

void CoordonatorUnit::MapStep(vector<string> input_files_paths) {

	// Initializare workeri etapa Map
	//cout << "Coordonator[ 0 ]: Configure Workers to Map state ...\n"; 
	logger.Log("Coordonator[ 0 ]: Configure Workers to Map state ...");
	sprintf(message, "map");
	BroadcastWorkersString(message);

	// Send intermediar file
	logger.Log("Coordonator[ 0 ]: Send to workers intermediar file ...");
	sprintf(message, intermediar_path.c_str());
	BroadcastWorkersString(message);

	//cout << "Coordonator[ 0 ]: Round robin files to workers ...\n";
	logger.Log("Coordonator[ 0 ]: Round robin files to workers ...");
	RoundRobinTasks(input_files_paths);

	//cout << "Coordonator[ 0 ]: Waiting workers to finish Mapping ...\n"; 
	logger.Log("Coordonator[ 0 ]: Waiting workers to finish Mapping ...");
	WaitForWorkers();

}

void CoordonatorUnit::ReduceStep()
{
	// Initializare workeri etapa Reduce
	logger.Log("Coordonator[ 0 ]: Configure Workers to Reduce state ...");
	sprintf(message, "reduce");
	BroadcastWorkersString(message);

	// Trimite numarul de workeri
	logger.Log("Coordonator[ 0 ]: Send number of workers ...");
	BroadcastWorkersInt(workers);

	// Trimite fisierul intermediar
	logger.Log("Coordonator[ 0 ]: Send intermediar file to workers ...");
	sprintf(message, intermediar_path.c_str());
	BroadcastWorkersString(message);

	// Trimite fisierul de output
	logger.Log("Coordonator[ 0 ]: Send output file to workers ...");
	sprintf(message, output_path.c_str());
	BroadcastWorkersString(message);

	// Creare lista de fisiere de procesat
	logger.Log("Coordonator[ 0 ]: Process intermediar files' names ...");
	vector<string> to_process;
	string s = "a";
	for (char c = 'a'; c <= 'z'; c++) {

		s[0] = c;

		// Creaza nume fisier
		string file = s + ".txt";

		// Adauga in lista
		to_process.push_back(file);
	}
	for (char c = '0'; c <= '9'; c++) {

		s[0] = c;

		// Creaza nume fisier
		string file = s + ".txt";

		// Adauga in lista
		to_process.push_back(file);
	}

	// Round Robin fisierele de lucru
	logger.Log("Coordonator[ 0 ]: Round robin files to workers ...");
	RoundRobinTasks(to_process);

	// Wait for the workers to finish
	logger.Log("Coordonator[ 0 ]: Waiting workers to finish Mapping ...");
	WaitForWorkers();

	// Collect answers
	CollectAnswers(to_process);
}

void CoordonatorUnit::CollectAnswers(vector<string> files_to_open) {

	// Buffer
	char buffer[512];

	// Deschid fisier in care scriu rezultate
	ofstream fout(output_path + "\\result.txt");
	
	// Pentru fiecare fisier in parte
	for(auto file_name : files_to_open)
	{

		ifstream fin(output_path + "\\" + file_name);
		while (fin.getline(buffer, 511))
		{
			fout.write(buffer, strlen(buffer));
			fout << '\n';
		}
		fin.close();
	}

	fout.close();
}
