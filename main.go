package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/log"
	"github.com/go-playground/log/handlers/console"
	"github.com/pkg/errors"
	"gopkg.in/telegram-bot-api.v4"
)

var (
	databases   []string  = []string{ /*"postgres",*/ "cockroachdb"}
	nodescounts []string  = []string{ /*"1",*/ "3"}
	workloads   []string  = []string{ /*"a", "b", "c", "f", "d",*/ "d", "d"}
	threads     []string  = []string{ /*"3", "9", "15", "21", "27", "33",*/ "39", "45"}
	now         time.Time = time.Now()
	csv         *os.File
	pwd         string
)

const (
	ycsbHome string = "/home/matjazmav/ycsb-0.12.0"
	botToken string = "593118488:AAGtXOM-g9aM7_L-3r-nuKgVjpqDj1rZ1oA"
	chatId   int64  = 473824114
)

type TelegramBotHandler struct {
	bot *tgbotapi.BotAPI
}

func (h *TelegramBotHandler) Log(e log.Entry) {
	message := fmt.Sprintf("%s\n%s", e.Level.String(), e.Message)
	h.bot.Send(tgbotapi.NewMessage(chatId, message))
}

func main() {
	var err error

	pwd, err = os.Getwd()
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "Can not get pwd."))
	}

	// Register console logger
	cLog := console.New(true)
	log.AddHandler(cLog, log.AllLevels...)

	// Create telegram bot client
	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "Telegram bot authentication failed."))
	}

	// Register telegram logger
	tgLog := new(TelegramBotHandler)
	tgLog.bot = bot
	log.AddHandler(tgLog, log.NoticeLevel, log.WarnLevel, log.ErrorLevel, log.PanicLevel, log.AlertLevel, log.FatalLevel)

	// Open CSV file
	csvFilePath := pwd + "/results/" + now.Format("20060102150405") + ".csv"
	csv, err = os.OpenFile(csvFilePath, os.O_CREATE|os.O_APPEND|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "Can not open CSV file."))
	}
	defer csv.Close()

	// Add CSV header
	_, err = csv.WriteString(fmt.Sprintf("%s\n", BuildCsvHeader()))
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "Can not write to header to CSV file."))
	}

	// Run benchmarks
	dbInitialized := false
	for _, database := range databases {
		for _, nodescount := range nodescounts {
			for _, workload := range workloads {

				// Restore data & start docker stack
				if !dbInitialized {
					if err = restoreData(database, nodescount); err != nil {
						clearData(database)
						log.Fatalf("%+v", errors.Wrap(err, "Can not restore data."))
					}
					if err = startStack(database, nodescount); err != nil {
						stopStack(database, nodescount)
						clearData(database)
						log.Fatalf("%+v", errors.Wrap(err, "Can not start stack."))
					}
					dbInitialized = true
				}

				// Run workload for variety of threads & targets
				for _, threadcount := range threads {
					log.Infof("Database: %s\nNodes: %s\nWorkload: %s\nThreads: %s\n", database, nodescount, workload, threadcount)

					if err = ycsbRun(database, nodescount, workload, threadcount); err != nil {
						stopStack(database, nodescount)
						clearData(database)
						log.Fatalf("%+v", errors.Wrap(err, "Can not run YCSB benchmark."))
					}
					time.Sleep(60 * time.Second)
				}
			}

			// Stop docker stack & clear data
			if err = stopStack(database, nodescount); err != nil {
				log.Fatalf("%+v", errors.Wrap(err, "Can not stop stack."))
			}
			if err = clearData(database); err != nil {
				log.Fatalf("%+v", errors.Wrap(err, "Can not clear data."))
			}
			dbInitialized = false

		}

	}
}

func startStack(database string, nodescount string) error {
	log.Info("Deploying stack...")

	var err error

	args := []string{
		`stack`,
		`up`,
		`--compose-file`,
		pwd + `/stacks/` + database + `-n` + nodescount + `.yml`,
		database + `_n` + nodescount,
	}

	cmd := exec.Command(`docker`, args...)

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "Can not start command.")
	}

	err = cmd.Wait()
	if err != nil {
		return errors.Wrap(err, "Can not wait command.")
	}

	time.Sleep(30 * time.Second)
	log.Info("DONE.")
	return nil
}

func stopStack(database string, nodescount string) error {
	log.Info("Removing stack...")

	var err error

	args := []string{
		`stack`,
		`rm`,
		database + `_n` + nodescount,
	}

	cmd := exec.Command(`docker`, args...)

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "Can not start command.")
	}

	err = cmd.Wait()
	if err != nil {
		return errors.Wrap(err, "Can not wait command.")
	}

	time.Sleep(10 * time.Second)
	log.Info("DONE.")
	return nil
}

func clearData(database string) error {
	log.Info("Cleaning data...")

	var err error

	args := []string{
		`mav-swarm`,
		`--become`,
		`-m`,
		`file`,
		`-a`,
		`state=absent path=/volumes/` + database + `/data`,
	}

	cmd := exec.Command(`ansible`, args...)

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "Can not start command.")
	}

	err = cmd.Wait()
	if err != nil {
		return errors.Wrap(err, "Can not wait command.")
	}

	log.Info("DONE.")
	return nil
}

func restoreData(database string, nodescount string) error {
	log.Info("Restoring data...")

	var err error

	args := []string{
		`mav-swarm`,
		`--become`,
		`-m`,
		`shell`,
		`-a`,
		`cp -a /volumes/` + database + `/data-template-ycsb-n` + nodescount + `-rc5M /volumes/` + database + `/data`,
	}

	cmd := exec.Command(`ansible`, args...)

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "Can not start command.")
	}

	err = cmd.Wait()
	if err != nil {
		return errors.Wrap(err, "Can not wait command.")
	}

	log.Info("DONE.")
	return nil
}

func ycsbRun(database string, nodescount string, workload string, threadcount string) error {
	log.Info("Running workload...")

	var err error

	// HACK: CRDB on 3 nodes uses 3 shards
	alignedThreadcount := threadcount
	if database == "cockroachdb" && nodescount == "3" {
		threads, _ := strconv.Atoi(threadcount)
		if threads%3 != 0 {
			return errors.New("Threads % 3 should be equal to 0.")
		}

		var alignedThreads int
		alignedThreads = threads / 3
		alignedThreadcount = strconv.Itoa(alignedThreads)
	}

	args := []string{
		"run",
		"jdbc",
		"-P",
		pwd + "/workloads/workload" + workload,
		"-P",
		pwd + "/configs/" + database + "-n" + nodescount + ".properties",
		"-p",
		"recordcount=5000000",
		"-p",
		"operationcount=5000000",
		"-p",
		"maxexecutiontime=60",
		"-threads",
		alignedThreadcount,
		//"-target",
		//target,
		//"-s", // print status every 10 seconds
	}

	cmd := exec.Command(ycsbHome+"/bin/ycsb", args...)
	cmd.Dir = ycsbHome
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "Can not get stdout pipe.")
	}

	// Create result struct
	result := new(Result)
	result.Time = time.Now()
	result.Database = database
	result.Workload = workload
	result.NodesCount, _ = strconv.Atoi(nodescount)
	result.ThreadsCount, _ = strconv.Atoi(threadcount)
	result.Duration = 60
	result.ReadResult = new(OperationResult)
	result.InsertResult = new(OperationResult)
	result.UpdateResult = new(OperationResult)
	result.ScanResult = new(OperationResult)
	result.RmwResult = new(OperationResult)

	scanner := bufio.NewScanner(stdout)
	go func(r *Result) {
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Println(line)

			if !strings.HasPrefix(line, "[") {
				continue
			}

			r.SetResult(line)
		}
	}(result)

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "Can not start command.")
	}

	err = cmd.Wait()
	if err != nil {
		return errors.Wrap(err, "Can not wait command.")
	}

	json, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return errors.Wrap(err, "Can not convert to JSON.")
	}

	log.Notice(string(json))

	_, err = csv.WriteString(fmt.Sprintf("%s\n", result.ToCsvRow()))
	if err != nil {
		return errors.Wrap(err, "Can not write result to CSV file.")
	}

	log.Info("DONE.")
	return nil
}
