#include <stdio.h>
#include <iostream>
#include <string>
#include <vector>
#include "mpi.h"
#include "CoordonatorUnit.h"
#include "Worker.h"

using namespace std;

int main(int argc, char* argv[]) {
	
	int my_rank; /* rank of process */
	int processes; /* number of processes */

	// Check if argc contains Input si Output directories
	if (argc != 3) {
		fprintf(stderr, "Linia de comanda trebuie sa contina directorul de intrare si cel de iesire.");
		return -1;
	}

	// Citeste path-urile pentru input si output
	string input_path = argv[1];
	string output_path = argv[2];

	// Creaza logger
	Logger logger("Log");

	/* start up MPI */
	MPI_Init(&argc, &argv);

	/* find out process rank */
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

	/* find out number of processes */
	MPI_Comm_size(MPI_COMM_WORLD, &processes);


#pragma region Program

	if (my_rank == 0) {
		CoordonatorUnit coordonator(processes, input_path, output_path, logger);
		coordonator.MapReduce(); 
	}
	else {
		Worker worker(my_rank, logger);
		worker.MainLoop();
	}
	
#pragma endregion Program


	MPI_Finalize();
	return 0;
}