#include <iostream>
#include <string>
#include <filesystem>
#include <fstream>
#include <regex>
#include <map>
#include "Worker.h"


using namespace std;
namespace fs = filesystem;

Worker::Worker(int my_rank, Logger logger) :my_rank(my_rank), logger(logger)
{
}

void Worker::MainLoop()
{
	bool is_working = true;
	// The loop
	while (is_working) {

		// Wait for command
		MPI_Recv(message, 100, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
		string command(message);

		// Analiza mesaj
		// Comanda de iesire
		if (command == "exit") {

			//cout << "Worker[ " << my_rank << " ]: Exit\n";
			logger.Log("Worker[ " + to_string(my_rank) + " ]: Exit");
			is_working = false;
			break;
		}

		// Comanda de initializare etapa Map
		if (command == "map") {

			//cout << "Worker[ " << my_rank << " ]: " << command << "\n";
			logger.Log("Worker[ " + to_string(my_rank) + " ]: map");

			// Primesc fisierul intermediar
			MPI_Recv(message, 100, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
			string intermediar_dir = message;

			// Fisierul de lucru trebuie setat inainte de apelul MapLoop()
			string worker_dir = intermediar_dir + "\\worker_" + to_string(my_rank);

			// Primeste fisiere pe rand si le prelucreaza
			// La finalul prelucrarilor, trimite un mesaj de finalizare etapa
			MapLoop(worker_dir);

			continue;
		}

		// Comanda de initializare etapa Reduce
		if (command == "reduce") {

			//cout << "Worker[ " << my_rank << " ]: " << command << "\n";
			logger.Log("Worker[ " + to_string(my_rank) + " ]: reduce");

			// Preia numarul de workeri existenti
			int existing_workers;
			MPI_Recv(&existing_workers, 1, MPI_INT, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);

			// Primesc fisierul intermediar
			MPI_Recv(message, 100, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
			string intermediar_dir = message;

			// Primeste fisierul de output
			MPI_Recv(message, 100, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
			string output_file(message);

			// Primeste fisiere pe rand si le prelucreaza
			// La finalul prelucrarilor, trimite un mesaj de finalizare etapa
			ReduceLoop(existing_workers, intermediar_dir, output_file);

			continue;
		}
	}
}

void Worker::MapLoop(string worker_dir)
{
	// Create worker directory
	fs::create_directory(worker_dir);

	// Primeste numarul de fisiere procesate
	// Wait for file path
	int num_files;
	MPI_Recv(&num_files, 1, MPI_INT, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);

	// Bucla pentru map
	for (int i = 0; i < num_files; i++) {

		// Wait for file path
		MPI_Recv(message, 100, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
		string file_path(message);

		//cout << "Worker[ " << my_rank << " ]: Proceseaza \"" + file_path + "\"\n";
		logger.Log("Worker[ " + to_string(my_rank) + " ]: Proceseaza \"" + file_path + "\"");

		// Procesare fisier
		FileMapper(file_path, worker_dir);

		//cout << "Worker[ " << my_rank << " ]: Termina \"" + file_path + "\"\n";
		logger.Log("Worker[ " + to_string(my_rank) + " ]: Termina \"" + file_path + "\"");
	}

	// Trimite la coordonator terminarea taskului
	SendFinishedTaskMessage();

}

void Worker::FileMapper(string path, string worker_dir)
{
	// Afla numele fisierului
	//	- optimizare spatiu stocare fisiere intermediare
	string file_name;
	smatch m;
	if (regex_search(path, m, regex("[0-9]+.txt$")))
		file_name = m[0];
	else 
		file_name = path;

	// Map cu streamurile existente
	map<string, ofstream*> streams;

	try {
		// Deschid fisierul respectiv
		ifstream fin(path);

		// Citesc din fisier cuvant cu cuvant din fisier
		string word;
		while (fin >> word) {

			// Lowercase
			transform(word.begin(), word.end(), word.begin(), ::tolower);

			// Trim word using regex
			word = regex_replace(word, regex("^[^a-z0-9]+|[^a-z0-9]+$"), "");

			// If the word is Empty jump to next
			if (word.empty()) {
				continue;
			}

			// Deschid fisierul de scriere
			string output_file = worker_dir + "\\" + word[0] + ".txt";
			ofstream *fout;

			// Daca exista fisierul de scriere
			if (streams.find(output_file) != streams.end()) {

				fout = streams.at(output_file);
			}
			// Nu exista fisier, il creem
			else {

				// Creez ofstream
				ofstream* f = new ofstream(output_file, ios_base::app);

				// Adaug in map
				pair<string, ofstream*> writer(output_file, f);
				streams.insert(writer);

				fout = f;
			}

			// Creaza o pereche {word file\n} pe care o ataseaza fisierului {word[0]}.txt
			(*fout) << word << " " << file_name << "\n";
		}

		// Inchiderea fisierelor
		for (map<string, ofstream*>::iterator it = streams.begin(); it != streams.end(); it ++) {
			(*(it->second)).close();
		}

	}
	catch(exception e){
		//cout << "Worker[ " << my_rank << " ]: Raised exception: " << e.what() << '\n';
		logger.Log("Worker[ " + to_string(my_rank) + " ]: Raised exception: " + e.what());
	}
}

void Worker::SendFinishedTaskMessage()
{
	// Seteaza mesajul
	sprintf(message, "end_task");

	// Trimite la coordonator
	MPI_Send(message, strlen(message) + 1, MPI_CHAR, coordonator, tag, MPI_COMM_WORLD);
}

void Worker::ReduceLoop(int existing_workers, string intermediar_file, string output_file)
{
	// Primeste numarul de fisiere procesate
	// Wait for file path
	int num_files;
	MPI_Recv(&num_files, 1, MPI_INT, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);

	// Bucla pentru reduce
	for (int i = 0; i < num_files; i++) {

		// Wait for file path
		MPI_Recv(message, 100, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
		string file_name(message);

		//cout << "Worker[ " << my_rank << " ]: Proceseaza \"" + file_path + "\"\n";
		logger.Log("Worker[ " + to_string(my_rank) + " ]: Proceseaza \"" + file_name + "\"");

		// Procesare fisier
		FileReducer(existing_workers, file_name, intermediar_file, output_file);

		//cout << "Worker[ " << my_rank << " ]: Termina \"" + file_path + "\"\n";
		logger.Log("Worker[ " + to_string(my_rank) + " ]: Termina \"" + file_name + "\"");
	}

	// Trimite la coordonator terminarea taskului
	SendFinishedTaskMessage();
}

void Worker::FileReducer(int existing_workers, string file_name, string intermediar_file, string output_file)
{
	// Create map structure
	// { <word> : { <fileName> : <count> } }
	map<string, map<string, int> > reduce_map;

	// Pentru fiecare worker
	for (int worker = 1; worker <= existing_workers; worker++) {

		// Current working directory
		string current_file = intermediar_file + "\\worker_" + to_string(worker) + "\\" + file_name;

		// Verifica daca directorul fisierul exista
		if (!fs::exists(current_file)) {
			continue;
		}

		// Open the file
		ifstream fin(current_file);

		// Variables
		string word;
		string from_file;

		// Read elements
		while (fin >> word >> from_file) {

			// Daca nu este in structura
			auto exists_iter = reduce_map.find(word);
			if(exists_iter == reduce_map.end()){

				// Creaza instanta si adauga in structura
				map<string, int> correspondent;
				correspondent.emplace(from_file, 1);

				// Adaugam in map inregistrarea
				reduce_map.emplace(word, correspondent);

				continue;
			}

			// Exista in structura
			// Verifica daca exista fisierul in componenta map
			auto file_iter = exists_iter->second.find(from_file);

			// Daca nu exista fisierul, adaugam in map
			if (file_iter == exists_iter->second.end()) {
				exists_iter->second.emplace(from_file, 1);
				continue;
			}

			// Exista fisierul, modificam valoarea
			file_iter->second ++;
		}

	}

	// Create output file
	ofstream fout(output_file + "\\" + file_name);

	// Afiseaza elementele structurii
	for (auto element : reduce_map) {

		// Afiseaza cuvantul
		fout << element.first;

		// Parcurge lista de fisiere
		for (auto file_elem : element.second) {

			fout << " (" << file_elem.first << ", " << file_elem.second << ")";
		}

		fout << '\n';
	}

	fout.close();
}
