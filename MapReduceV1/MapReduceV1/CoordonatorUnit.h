#pragma once
#include <string>
#include <vector>
#include "Logger.h"
#define ONLY_MAP 0
#define ONLY_REDUCE 1
#define BOTH 2

using namespace std;

class CoordonatorUnit {
private:
	// Modul de procesare
	const int PROCESS_ONLY = BOTH;

	// Path catre directorul de lucru
	const string intermediar_path = "Intermediar";

	// Rank-ul task-ului coordonator
	const int my_rank = 0;

	// Mesajul transmis
	char message[100];

	// Numarul de workeri existenti
	int workers;

	// Fisierele de lucru
	string input_path;
	string output_path; 

	// Logger
	Logger logger;

public:
	CoordonatorUnit(int processes, string input_path, string output_path, Logger logger);

	void BroadcastWorkersString(char* message);

	void BroadcastWorkersInt(int value);

	void RoundRobinTasks(vector<string> tasks);

	void WaitForWorkers();

	void MapReduce();

	void MapStep(vector<string> input_files_paths);

	void ReduceStep();

	void CollectAnswers(vector<string> files_to_open);
};
