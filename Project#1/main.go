package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	// Shortest-job-first scheduling
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//SJFPrioritySchedule(os.Stdout, "Priority", processes)

	//

	//
	//RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
		Wait          int64
		Turnaround    int64
		Burst         int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		//serviceTime     int64
		start           int64
		totalWait       float64
		totalTurnaround float64
		//lastCompletion  float64
		schedule     = make([][]string, len(processes))
		gantt        = make([]TimeSlice, 0)
		time         int64     //time counter
		pCount       int       //counter for processes slice
		readyQueue   []Process //Queue for processes ready to be executed
		executedP    Process   // process that is currently being executed
		numProcesses int       = len(processes)
	)
	start = time
	outputTitle(w, title)
	fmt.Println(executedP)
	//fmt.Println(len(readyQueue))
	for {
		if numProcesses < 1 {
			break //numProcesses is set to total number of processes, each time one is finished executing this number will decrease
		}

		if pCount < len(processes) && time == processes[pCount].ArrivalTime { //once we have added all the processes to the ready queue we will stop using
			//add process to queue
			readyQueue = append(readyQueue, processes[pCount])
			readyQueue[len(readyQueue)-1].Burst = processes[pCount].BurstDuration
			//fmt.Println(readyQueue[len(readyQueue)-1], " added at time = ", time, "its arrival time is", readyQueue[len(readyQueue)-1].ArrivalTime)
			pCount++

		}
		//sort readyQueue so shortest BurstDuration is 1st item in queue
		tempPID := readyQueue[0].ProcessID
		sort.SliceStable(readyQueue, func(i, j int) bool {
			return readyQueue[i].BurstDuration < readyQueue[j].BurstDuration
		})
		time++

		readyQueue[0].BurstDuration--
		for i := range readyQueue {
			if i != 0 {
				readyQueue[i].Wait++
			}
		}
		if tempPID != readyQueue[0].ProcessID {

			gantt = append(gantt, TimeSlice{
				PID:   readyQueue[0].ProcessID,
				Start: start,
				Stop:  time,
			})
			start = time
		}

		if readyQueue[0].BurstDuration < 1 {
			//fmt.Println(readyQueue[0].ProcessID, "has finished executing")
			totalWait += float64(readyQueue[0].Wait)
			turnaround := readyQueue[0].Wait + readyQueue[0].Burst
			totalTurnaround += float64(turnaround)
			schedule[readyQueue[0].ProcessID-1] = []string{
				fmt.Sprint(readyQueue[0].ProcessID),
				fmt.Sprint(readyQueue[0].Priority),
				fmt.Sprint(readyQueue[0].Burst),
				fmt.Sprint(readyQueue[0].ArrivalTime),
				fmt.Sprint(readyQueue[0].Wait),
				fmt.Sprint(turnaround),
				fmt.Sprint(time),
			}
			if len(gantt) > 1 && gantt[len(gantt)-1].PID == readyQueue[0].ProcessID {
				gantt[len(gantt)-1].Stop = time
			} else {
				gantt = append(gantt, TimeSlice{
					PID:   readyQueue[0].ProcessID,
					Start: start,
					Stop:  time,
				})
			}

			start = time

			if len(readyQueue) > 1 {
				//pop front of queue if there is more than 1 process in queue
				executedP, readyQueue = readyQueue[0], readyQueue[1:]
			}
			numProcesses--
		}

	}
	avgWait := totalWait / float64(pCount)
	avgTurnaround := totalTurnaround / float64(pCount)
	avgThroughput := float64(pCount) / float64(time)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, avgWait, avgTurnaround, avgThroughput)

}

//func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
//	outputTitle(w, title)
//}

//

//
//func RRSchedule(w io.Writer, title string, processes []Process) { }

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
