/*****************************************************************************
*
*	File			: producer.go
*
* 	Created			: 27 Aug 2021
*
*	Description		:
*
*	Modified		: 27 Aug 2021	- Start
*					: 24 Jan 2023   - Mod for applab sandbox
*					: 20 Feb 2023	- repckaged for TFM 2.0 load generation, we're creating fake FS engineResponse messages onto a
*					:				- Confluent Kafka topic
*
*	By				: George Leonard (georgelza@gmail.com)
*
*	jsonformatter 	: https://jsonformatter.curiousconcept.com/#
*
*****************************************************************************/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/google/uuid"

	"github.com/TylerBrock/colorjson"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	glog "google.golang.org/grpc/grpclog"

	// Prometheus
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	// My Types/Structs/functions
	"cmd/seed"
	"cmd/types"
)

// Private Structs
type tp_general struct {
	hostname     string
	debuglevel   int
	loglevel     string
	logformat    string
	testsize     int
	sleep        int
	json_to_file int
	output_path  string
}

type tp_kafka struct {
	bootstrapservers  string
	topicname         string
	numpartitions     int
	replicationfactor int
	retension         string
	parseduration     string
	security_protocol string
	sasl_mechanisms   string
	sasl_username     string
	sasl_password     string
	flush_interval    int
}

type metrics struct {
	info          *prometheus.GaugeVec
	req_processed *prometheus.CounterVec
	sql_duration  *prometheus.HistogramVec
	rec_duration  *prometheus.HistogramVec
	api_duration  *prometheus.HistogramVec
}

var (
	grpcLog glog.LoggerV2
	reg     = prometheus.NewRegistry()
	m       = NewMetrics(reg)
)

func init() {

	// Keeping it very simple
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

	grpcLog.Infoln("###############################################################")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   File      : producer.go")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#	Comment   : Fake Kafka Producer - Applab Example")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   By        : George Leonard (georgelza@gmail.com)")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Date/Time :", time.Now().Format("27-01-2023 - 15:04:05"))
	grpcLog.Infoln("#")
	grpcLog.Infoln("###############################################################")
	grpcLog.Infoln("")
	grpcLog.Infoln("")

}

func NewMetrics(reg prometheus.Registerer) *metrics {

	m := &metrics{
		info: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "txn_count",
			Help: "Target amount for completed requests",
		}, []string{"batch"}),

		req_processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fs_etl_operations_count",
			Help: "Number of completed requests.",
		}, []string{"batch"}),

		sql_duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "fs_sql_duration_seconds",
			Help: "Duration of the sql requests",
			// 4 times larger apdex status
			// Buckets: prometheus.ExponentialBuckets(0.1, 1.5, 5),
			// Buckets: prometheus.LinearBuckets(0.1, 5, 5),
			Buckets: []float64{0.1, 0.15, 0.2, 0.25, 0.3},
		}, []string{"batch"}),

		rec_duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "fs_etl_operations_seconds",
			Help: "Duration of the entire requests",

			Buckets: []float64{0.1, 0.15, 0.2, 0.25, 0.3},
		}, []string{"batch"}),

		api_duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fs_api_duration_seconds",
			Help:    "Duration of the api requests",
			Buckets: []float64{0.1, 0.15, 0.2, 0.25, 0.3},
		}, []string{"batch"}),
	}

	reg.MustRegister(m.info, m.req_processed, m.sql_duration, m.rec_duration, m.api_duration)

	return m
}

func CreateTopic(props tp_kafka) {

	cm := kafka.ConfigMap{
		"bootstrap.servers":       props.bootstrapservers,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
	}

	if props.sasl_mechanisms != "" {
		cm["sasl.mechanisms"] = props.sasl_mechanisms
		cm["security.protocol"] = props.security_protocol
		cm["sasl.username"] = props.sasl_username
		cm["sasl.password"] = props.sasl_password
		grpcLog.Infoln("Security Authentifaction configured in ConfigMap")

	}
	grpcLog.Infoln("Basic Client ConfigMap compiled")

	adminClient, err := kafka.NewAdminClient(&cm)
	if err != nil {
		grpcLog.Fatalln("Admin Client Creation Failed: %s", err)

	}
	grpcLog.Infoln("Admin Client Created Succeeded")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDuration, err := time.ParseDuration(props.parseduration)
	if err != nil {
		grpcLog.Fatalln(fmt.Sprintf("Error Configuring maxDuration via ParseDuration: %s", props.parseduration))

	}
	grpcLog.Infoln("Configured maxDuration via ParseDuration")

	results, err := adminClient.CreateTopics(ctx,
		[]kafka.TopicSpecification{{
			Topic:             props.topicname,
			NumPartitions:     props.numpartitions,
			ReplicationFactor: props.replicationfactor}},
		kafka.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		grpcLog.Fatalln("Problem during the topic creation: %v", err)
	}

	// Check for specific topic errors
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError &&
			result.Error.Code() != kafka.ErrTopicAlreadyExists {

			grpcLog.Fatalln(fmt.Sprintf("Topic Creation Failed for %s: %v", result.Topic, result.Error.String()))

		} else {
			grpcLog.Infoln(fmt.Sprintf("Topic Creation Succeeded for %s", result.Topic))

		}
	}

	adminClient.Close()
	grpcLog.Infoln("")

}

func loadGeneralProps() (vGeneral tp_general) {

	// Lets identify ourself - helpful concept in a container environment.
	var err interface{}

	vHostname, err := os.Hostname()
	if err != nil {
		grpcLog.Errorln("Can't retrieve hostname %s", err)

	}
	vGeneral.hostname = vHostname

	vGeneral.logformat = os.Getenv("LOG_FORMAT")
	vGeneral.loglevel = os.Getenv("LOG_LEVEL")

	// Lets manage how much we print to the screen
	vGeneral.debuglevel, err = strconv.Atoi(os.Getenv("DEBUGLEVEL"))
	if err != nil {
		grpcLog.Errorln("String to Int convert error: %s", err)

	}

	// Lets manage how much we print to the screen
	vGeneral.testsize, err = strconv.Atoi(os.Getenv("TESTSIZE"))
	if err != nil {
		grpcLog.Errorln("String to Int convert error: %s", err)

	}

	// Lets manage how much we print to the screen
	vGeneral.sleep, err = strconv.Atoi(os.Getenv("SLEEP"))
	if err != nil {
		grpcLog.Errorln("String to Int convert error: %s", err)

	}

	vGeneral.json_to_file, err = strconv.Atoi(os.Getenv("JSONTOFILE"))
	if err != nil {
		grpcLog.Errorln("String to Int convert error: %s", err)

	}

	if vGeneral.json_to_file == 1 {

		path, err := os.Getwd()
		if err != nil {
			grpcLog.Errorln("Problem retrieving current path: %s", err)
		}
		vGeneral.output_path = path + "/" + os.Getenv("OUTPUT_PATH")

	}

	grpcLog.Info("****** General Parameters *****")
	grpcLog.Info("Hostname is\t\t\t", vGeneral.hostname)
	grpcLog.Info("Debug Level is\t\t", vGeneral.debuglevel)
	grpcLog.Info("Log Level is\t\t\t", vGeneral.loglevel)
	grpcLog.Info("Log Format is\t\t\t", vGeneral.logformat)
	grpcLog.Info("Sleep Duration is\t\t", vGeneral.sleep)
	grpcLog.Info("Test Batch Size is\t\t", vGeneral.testsize)
	grpcLog.Info("Dump JSON to file is\t\t", vGeneral.json_to_file)
	grpcLog.Info("Output path is\t\t", vGeneral.output_path)
	grpcLog.Info("")

	return vGeneral
}

func loadKafkaProps() (vKafka tp_kafka) {

	var err interface{}

	// Broker Configuration
	vKafka.bootstrapservers = fmt.Sprintf("%s:%s", os.Getenv("kafka_bootstrap_servers"), os.Getenv("kafka_bootstrap_port"))
	vKafka.topicname = os.Getenv("kafka_topic_name")
	vKafka_NumPartitions := os.Getenv("kafka_num_partitions")
	vKafka_ReplicationFactor := os.Getenv("kafka_replication_factor")
	vKafka.retension = os.Getenv("kafka_retension")
	vKafka.parseduration = os.Getenv("kafka_parseduration")

	vKafka.security_protocol = os.Getenv("kafka_security_protocol")
	vKafka.sasl_mechanisms = os.Getenv("kafka_sasl_mechanisms")
	vKafka.sasl_username = os.Getenv("kafka_sasl_username")
	vKafka.sasl_password = os.Getenv("kafka_sasl_password")

	vKafka.flush_interval, err = strconv.Atoi(os.Getenv("kafka_flushinterval"))
	if err != nil {
		grpcLog.Error("String to Int convert error: %s", err)

	}

	// Lets manage how much we prnt to the screen
	vKafka.numpartitions, err = strconv.Atoi(vKafka_NumPartitions)
	if err != nil {
		grpcLog.Error("vKafka_NumPartitions, String to Int convert error: %s", err)

	}

	// Lets manage how much we prnt to the screen
	vKafka.replicationfactor, err = strconv.Atoi(vKafka_ReplicationFactor)
	if err != nil {
		grpcLog.Error("vKafka_ReplicationFactor, String to Int convert error: %s", err)

	}

	grpcLog.Info("****** Kafka Connection Parameters *****")
	grpcLog.Info("Kafka bootstrap Server is\t", vKafka.bootstrapservers)
	grpcLog.Info("Kafka Topic is\t\t", vKafka.topicname)
	grpcLog.Info("Kafka # Parts is\t\t", vKafka_NumPartitions)
	grpcLog.Info("Kafka Rep Factor is\t\t", vKafka_ReplicationFactor)
	grpcLog.Info("Kafka Retension is\t\t", vKafka.retension)
	grpcLog.Info("Kafka ParseDuration is\t", vKafka.parseduration)
	grpcLog.Info("")
	grpcLog.Info("Kafka Flush Size is\t\t", vKafka.flush_interval)

	grpcLog.Info("")

	return vKafka
}

// Pretty Print JSON string
func prettyJSON(ms string) {

	var obj map[string]interface{}
	//json.Unmarshal([]byte(string(m.Value)), &obj)
	json.Unmarshal([]byte(ms), &obj)

	// Make a custom formatter with indent set
	f := colorjson.NewFormatter()
	f.Indent = 4

	// Marshall the Colorized JSON
	result, _ := f.Marshal(obj)
	fmt.Println(string(result))

}

// This was originally in main body, pulled it out here to show how we can construct the payload or parts
// in external sections and then bring it back
func contructPaymentNrt() (t_PaymentNrt map[string]interface{}, cTenant string, cMerchant string) {

	// We just using gofakeit to pad the json document size a bit.
	//
	// https://github.com/brianvoe/gofakeit
	// https://pkg.go.dev/github.com/brianvoe/gofakeit

	gofakeit.Seed(time.Now().UnixNano())
	gofakeit.Seed(0)

	g_person := gofakeit.Person()

	// Commenting this out to reduce the g_person JSON object
	/*
		g_address := gofakeit.Address()
		g_ccard := gofakeit.CreditCard()
		g_job := gofakeit.Job()
		g_contact := gofakeit.Contact()

		address := &types.TPaddress{
			Streetname: g_address.Street,
			City:       g_address.City,
			State:      g_address.State,
			Zip:        g_address.Zip,
			Country:    g_address.Country,
			Latitude:   g_address.Latitude,
			Longitude:  g_address.Longitude,
		}

		ccard := &types.TPccard{
			Type:   g_ccard.Type,
			Number: g_ccard.Number,
			Exp:    g_ccard.Exp,
			Cvv:    g_ccard.Cvv,
		}

		job := &types.TPjobinfo{
			Company:    g_job.Company,
			Title:      g_job.Title,
			Descriptor: g_job.Descriptor,
			Level:      g_job.Level,
		}

		contact := &types.TPcontact{
			Email: g_contact.Email,
			Phone: g_contact.Phone,
		}
	*/
	tperson := &types.TPperson{
		Ssn:       g_person.SSN,
		Firstname: g_person.FirstName,
		Lastname:  g_person.LastName,
		//				Gender:       g_person.Gender,
		//				Contact:      *contact,
		//				Address:      *address,
		//				Ccard:        *ccard,
		//				Job:          *job,
		Created_date: time.Now().Format("2006-01-02T15:04:05"),
	}

	// Lets start building the various bits that comprises the engineResponse JSON Doc
	// This is the original inbound event, will be 2, 1 for outbound bank and 1 for inbound bank out
	//

	cTransType := seed.GetTransType()[gofakeit.Number(1, 4)]
	cDirection := seed.GetDirection()[gofakeit.Number(1, 2)]
	nAmount := fmt.Sprintf("%9.2f", gofakeit.Price(0, 999999999))

	cTenant = seed.GetTenants()[gofakeit.Number(1, 15)] // tenants
	cMerchant = seed.GetEntityId()[gofakeit.Number(1, 26)]

	t_amount := &types.TPamount{
		BaseCurrency: "zar",
		BaseValue:    nAmount,
		Burrency:     "zar",
		Value:        nAmount,
	}

	//
	// This would all be coming from the S3 bucket via a source connector,
	// so instead of generating the data we would simply read the
	// JSON document and push it to Kafka topic.
	//

	// We ust showing 2 ways to construct a JSON document to be Marshalled, this is the first using a map/interface,
	// followed by using a set of struct objects added together.
	t_PaymentNrt = map[string]interface{}{
		"accountAgentId":               strconv.Itoa(rand.Intn(6)),
		"accountEntityId":              strconv.Itoa(rand.Intn(6)),
		"accountId":                    strconv.Itoa(gofakeit.CreditCardNumber()),
		"amount":                       t_amount,
		"counterpartyEntityId":         strconv.Itoa(gofakeit.Number(0, 9999)),
		"counterpartyId":               strconv.Itoa(gofakeit.Number(10000, 19999)),
		"counterpartyName":             *tperson,
		"customerEntityId":             "customerEntityId_1",
		"customerId":                   "customerId_1",
		"direction":                    cDirection,
		"eventId":                      strconv.Itoa(gofakeit.CreditCardNumber()),
		"eventTime":                    time.Now().Format("2006-01-02T15:04:05"),
		"eventType":                    "paymentRT",
		"fromId":                       strconv.Itoa(gofakeit.CreditCardNumber()),
		"instructedAgentId":            strconv.Itoa(gofakeit.Number(0, 4999)),
		"instructingAgentId":           strconv.Itoa(gofakeit.Number(5000, 9999)),
		"msgStatus":                    "msgStatus_1",
		"msgType":                      "msgType_1",
		"numberOfTransactions":         strconv.Itoa(gofakeit.Number(0, 99)),
		"paymentFrequency":             strconv.Itoa(gofakeit.Number(1, 12)),
		"paymentMethod":                "paymentMethod_1",
		"schemaVersion":                "1",
		"serviceLevelCode":             "serviceLevelCode_1",
		"settlementClearingSystemCode": "settlementClearingSystemCode_1",
		"settlementDate":               time.Now().Format("2006-01-02"),
		"settlementMethod":             "samos",
		"tenantId":                     []string{cTenant},
		"toId":                         strconv.Itoa(gofakeit.CreditCardNumber()),
		"transactionId":                uuid.New().String(),
		"transactionType":              cTransType,
	}

	return t_PaymentNrt, cTenant, cMerchant
}

// Query database and get the record set to work with
func fetchEFTRecords() (records []string, count int) {

	grpcLog.Info("**** Quering Backend database ****")

	////////////////////////////////
	// start a timer
	sTime := time.Now()

	// Execute a large sql #1 execute
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(10000) // if vGeneral.sleep = 10000, 10 second
	grpcLog.Info("EFT SQL Sleeping Millisecond - Simulating long database fetch...", n)
	time.Sleep(time.Duration(n) * time.Millisecond)

	// post to Prometheus
	m.sql_duration.WithLabelValues("eft").Observe(time.Since(sTime).Seconds())

	// Return pointer to recordset and counter of number of records
	count = 4252345123

	grpcLog.Info("**** Backend dataset retrieved ****")

	return records, count

}

func runEFTLoader() {

	// Initialize the vGeneral struct variable.
	vGeneral := loadGeneralProps()
	vKafka := loadKafkaProps()

	// Create admin client to create the topic if it does not exist
	grpcLog.Info("**** Confirm Topic Existence & Configuration ****")

	// Lets make sure the topic/s exist
	CreateTopic(vKafka)

	// --
	// Create Producer instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewProducer

	grpcLog.Info("**** Configure Client Kafka Connection ****")

	grpcLog.Info("Kafka bootstrap Server is\t", vKafka.bootstrapservers)

	cm := kafka.ConfigMap{
		"bootstrap.servers":       vKafka.bootstrapservers,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"client.id":               vGeneral.hostname,
	}
	grpcLog.Infoln("Basic Client ConfigMap compiled")

	if vKafka.sasl_mechanisms != "" {
		cm["sasl.mechanisms"] = vKafka.sasl_mechanisms
		cm["security.protocol"] = vKafka.security_protocol
		cm["sasl.username"] = vKafka.sasl_username
		cm["sasl.password"] = vKafka.sasl_password
		grpcLog.Infoln("Security Authentifaction configured in ConfigMap")

	}

	// Variable p holds the new Producer instance.
	p, err := kafka.NewProducer(&cm)

	// Check for errors in creating the Producer
	if err != nil {
		grpcLog.Errorf("😢Oh noes, there's an error creating the Producer! ", err)

		if ke, ok := err.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				grpcLog.Fatalf("😢 Can't create the producer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, err)

			default:
				grpcLog.Fatalf("😢 Can't create the producer (Kafka error code %d)\n\tError: %v\n", ec, err)

			}

		} else {
			// It's not a kafka.Error
			grpcLog.Fatalf("😢 Oh noes, there's a generic error creating the Producer! %v", err.Error())

		}
		// call it when you know it's broken
		os.Exit(1)

	} else {

		if vGeneral.debuglevel > 0 {
			grpcLog.Infoln("Created Kafka Producer instance :")
			grpcLog.Infoln("")
			grpcLog.Info("**** LETS GO Processing ****")
			grpcLog.Infoln("")
		}

		///////////////////////////////////////////////////////
		//
		// Successful connection established with Kafka Cluster
		//
		///////////////////////////////////////////////////////

		//
		// For signalling termination from main to go-routine
		termChan := make(chan bool, 1)
		// For signalling that termination is done from go-routine to main
		doneChan := make(chan bool)

		vFlush := 0

		////////////////////////////////////////////////////////////////////////
		// Lets fecth the records that need to be pushed to the fs api end point
		returnedRecs, todo_count := fetchEFTRecords()

		m.info.With(prometheus.Labels{"batch": "eft"}).Set(float64(todo_count)) // this will be the recordcount of the records returned by the sql query
		println(returnedRecs)                                                   // just doing this to prefer a unused error

		// As we're still faking it:
		todo_count = vGeneral.testsize // this will be recplaced by the value of todo_count from above.

		// now we loop through the results, building a json document based on FS requirements and then post it, for this code I'm posting to
		// Confluent Kafka topic, but it's easy to change to have it post to a API endpoint.

		// this is to keep record of the total run time
		vStart := time.Now()

		for count := 0; count < todo_count; count++ {

			// We're going to time every record and push that to prometheus
			txnStart := time.Now()

			// Eventually we will have a routine here that will not create fake data, but will rather read the data from
			// a file, do some checks and then publish the JSON payload structured Txn to the topic.

			// if 0 then sleep is disabled otherwise
			//
			// lets get a random value 0 -> vGeneral.sleep, then delay/sleep as up to that fraction of a second.
			// this mimics someone thinking, as if this is being done by a human at a keyboard, for batcvh file processing we don't have this.
			// ie if the user said 200 then it implies a randam value from 0 -> 200 milliseconds.

			if vGeneral.sleep != 0 {
				rand.Seed(time.Now().UnixNano())
				n := rand.Intn(vGeneral.sleep) // if vGeneral.sleep = 1000, then n will be random value of 0 -> 1000  aka 0 and 1 second
				if vGeneral.debuglevel >= 2 {
					fmt.Printf("Sleeping %d Millisecond...\n", n)
				}
				time.Sleep(time.Duration(n) * time.Millisecond)
			}

			// Build the entire JSON Payload document from the record fetched

			t_PaymentNrt, cTenant, cMerchant := contructPaymentNrt()

			cGetRiskStatus := seed.GetRiskStatus()[gofakeit.Number(1, 3)]

			// We cheating, going to use this same aggregator in both TPconfigGroups
			t_aggregators := []types.TPaggregator{
				types.TPaggregator{
					AggregatorId:   "agg1",
					AggregateScore: 0.3,
					MatchedBound:   0.1,
					Alert:          true,
					SuppressAlert:  false,
					Scores: types.TPscore{
						Models: []types.TPmodels{
							types.TPmodels{
								ModelId: "model1",
								Score:   0.51,
							},
							types.TPmodels{
								ModelId: "model2",
								Score:   0.42,
							},
							types.TPmodels{
								ModelId: "aggModel",
								Score:   0.2,
							},
						},
						Tags: []types.TPtags{
							types.TPtags{
								Tag:    "codes",
								Values: []string{"Dodgy"},
							},
						},
						Rules: []types.TPrules{
							types.TPrules{
								RuleId: "rule1",
								Score:  0.2,
							},
							types.TPrules{
								RuleId: "rule2",
								Score:  0.4,
							},
						},
					},
					OutputTags: []types.TPtags{
						types.TPtags{
							Tag:    "codes",
							Values: []string{"LowValueTrans"},
						},
					},
					SuppressedTags: []types.TPtags{
						types.TPtags{
							Tag:    "otherTag",
							Values: []string{"t3", "t4"},
						},
					},
				},

				types.TPaggregator{
					AggregatorId:   "agg2",
					AggregateScore: 0.2,
					MatchedBound:   0.4,
					Alert:          true,
					SuppressAlert:  false,
				},
			}

			t_entity := []types.TPentity{
				types.TPentity{
					TenantId:   cTenant,    // Tenant = a Bank, our customer
					EntityType: "merchant", // Entity is either a Merchant or the consumer, for outbound is it the consumer, inbound bank its normally the merchant, except when a reversal is actioned.
					EntityId:   cMerchant,  // if Merchant we push a seeded Merchant name into here, not sure how the consumer block would look

					OverallScore: types.TPoverallScore{
						AggregationModel: "aggModel",
						OverallScore:     0.2,
					},

					Models: []types.TPmodels{
						types.TPmodels{
							ModelId:    "model1",
							Score:      0.5,
							Confidence: 0.2,
						},
						types.TPmodels{
							ModelId:    "model2",
							Score:      0.1,
							Confidence: 0.9,
							ModelData: types.TPmodelData{
								Adata: "aData ASet",
							},
						},
						types.TPmodels{
							ModelId:    "aggModel",
							Score:      0.2,
							Confidence: 0.9,
							ModelData: types.TPmodelData{
								Adata: "aData BSet",
							},
						},
					},

					FailedModels: []string{"fm1", "fm2"},

					OutputTags: []types.TPtags{
						types.TPtags{
							Tag:    "codes",
							Values: []string{"highValueTransaction", "fraud"},
						},
						types.TPtags{
							Tag:    "otherTag",
							Values: []string{"t1", "t2"},
						},
					},

					RiskStatus: cGetRiskStatus,

					ConfigGroups: []types.TPconfigGroups{
						types.TPconfigGroups{
							Type:           "global",
							TriggeredRules: []string{"rule1", "rule2"},
							Aggregators:    t_aggregators,
						},
						types.TPconfigGroups{
							Type:           "analytical",
							Id:             "acg1	",
							TriggeredRules: []string{"rule1", "rule3"},
							Aggregators:    t_aggregators,
						},
						types.TPconfigGroups{
							Type:           "analytical",
							Id:             "acg2",
							TriggeredRules: []string{"rule1", "rule3"},
						},
						types.TPconfigGroups{
							Type:           "tenant",
							Id:             "tenant10",
							TriggeredRules: []string{"rule2", "rule3"},
						},
					},
				},

				types.TPentity{
					EntityType: "consumer",
					EntityId:   "consumer1",
				},
			}

			// In real live we'd define the object and then append the array items... of course !!!!
			// we're cheating as we're just creating volume to push onto Kafka and onto MongoDB store.
			t_versions := types.TPversions{
				ModelGraph: 4,
				ConfigGroups: []types.TPconfigGroups{
					types.TPconfigGroups{
						Type:    "global",
						Version: "1",
					},
					types.TPconfigGroups{
						Type:    "analytical",
						Version: "3",
						Id:      "acg1",
					},
					types.TPconfigGroups{
						Type:    "analytical",
						Version: "2",
						Id:      "acg2",
					},
					types.TPconfigGroups{
						Type:    "tenant",
						Id:      "tenant10",
						Version: "0",
					},
				},
			}

			t_engineResponse := types.TPengineResponse{
				Entities:         t_entity,
				JsonVersion:      4,
				OriginatingEvent: t_PaymentNrt,
				OutputTime:       time.Now().Format("2006-01-02T15:04:05"),
				ProcessorId:      "proc",
				Versions:         t_versions,
			}

			// Change/Marshal the t_engineResponse variable into an array of bytes required to be send
			valueBytes, err := json.Marshal(t_engineResponse)
			if err != nil {
				grpcLog.Errorln("Marchalling error: ", err)

			}

			kafkaMsg := kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &vKafka.topicname,
					Partition: kafka.PartitionAny,
				},
				Value: valueBytes,      // This is the payload/body thats being posted
				Key:   []byte(cTenant), // We us this to group the same transactions together in order, IE submitting/Debtor Bank.
			}

			if vGeneral.debuglevel > 1 {
				prettyJSON(string(valueBytes))
			}

			if vGeneral.json_to_file == 0 { // write to Kafka Topic

				// This is where we publish message onto the topic... on the Confluent cluster for now,
				// this will be replaced with a FS API post call

				apiStart := time.Now() // time the api call
				if e := p.Produce(&kafkaMsg, nil); e != nil {
					grpcLog.Errorln("😢 Darn, there's an error producing the message!", e.Error())

				}
				//determine the duration of the api call log to prometheus histogram
				m.rec_duration.WithLabelValues("nrt_eft").Observe(time.Since(apiStart).Seconds())

				//Fush every flush_interval loops
				if vFlush == vKafka.flush_interval {
					t := 10000
					if r := p.Flush(t); r > 0 {
						grpcLog.Errorln("Failed to flush all messages after %d milliseconds. %d message(s) remain", t, r)

					} else {
						if vGeneral.debuglevel >= 1 {
							grpcLog.Infoln(count, "/", vFlush, "Messages flushed from the queue")
						}
						vFlush = 0
					}
				}

			} else { // write the JSON to a file rather

				eventId := t_engineResponse.OriginatingEvent["eventId"]

				// define, contruct the file name
				loc := fmt.Sprintf("%s/%s.json", vGeneral.output_path, eventId)
				if vGeneral.debuglevel > 0 {
					grpcLog.Infoln(loc)

				}

				//...................................
				// Writing struct type to a JSON file
				//...................................
				// Writing
				// https://www.golangprograms.com/golang-writing-struct-to-json-file.html
				// https://www.developer.com/languages/json-files-golang/
				// Reading
				// https://medium.com/kanoteknologi/better-way-to-read-and-write-json-file-in-golang-9d575b7254f2
				fd, err := json.MarshalIndent(t_engineResponse, "", " ")
				if err != nil {
					grpcLog.Errorln("MarshalIndent error", err)

				}

				err = ioutil.WriteFile(loc, fd, 0644)
				if err != nil {
					grpcLog.Errorln("ioutil.WriteFile error", err)

				}
			}

			// We will decide if we want to keep this bit!!! or simplify it.
			//
			// Convenient way to Handle any events (back chatter) that we get
			go func() {
				doTerm := false
				for !doTerm {
					// The `select` blocks until one of the `case` conditions
					// are met - therefore we run it in a Go Routine.
					select {
					case ev := <-p.Events():
						// Look at the type of Event we've received
						switch ev.(type) {

						case *kafka.Message:
							// It's a delivery report
							km := ev.(*kafka.Message)
							if km.TopicPartition.Error != nil {
								grpcLog.Infoln("☠️ Failed to send message '%v' to topic '%v'\tErr: %v",
									string(km.Value),
									string(*km.TopicPartition.Topic),
									km.TopicPartition.Error)

							} else {
								if vGeneral.debuglevel > 2 {

									grpcLog.Infoln("✅ Message '%v' delivered to topic '%v'(partition %d at offset %d)",
										string(km.Value),
										string(*km.TopicPartition.Topic),
										km.TopicPartition.Partition,
										km.TopicPartition.Offset)
								}
							}

						case kafka.Error:
							// It's an error
							em := ev.(kafka.Error)
							grpcLog.Errorln("☠️ Uh oh, caught an error:\n\t%v", em)
							//logCtx.Errorf(fmt.Sprintf("☠️ Uh oh, caught an error:\n\t%v\n", em))

						}
					case <-termChan:
						doTerm = true

					}
				}
				close(doneChan)
			}()

			/////////////////////////////////////////////////////////////////////////////
			// Prometheus Metrics
			//
			// // increment a counter for number of requests processed, we can use this number with time to create a throughput graph
			m.req_processed.WithLabelValues("nrt_eft").Inc()

			// // determine the duration and log to prometheus histogram, this is for the entire build of payload and post
			m.api_duration.WithLabelValues("nrt_eft").Observe(time.Since(txnStart).Seconds())

			vFlush++

		}

		if vGeneral.debuglevel > 0 {
			grpcLog.Infoln("")
			grpcLog.Infoln("**** DONE Processing ****")
			grpcLog.Infoln("")
		}

		// --
		// Flush the Producer queue, t = TimeOut, 1000 = 1 second
		t := 10000
		if r := p.Flush(t); r > 0 {
			grpcLog.Errorln("Failed to flush all messages after %d milliseconds. %d message(s) remain", t, r)
			//logCtx.Errorf(fmt.Sprintf("\n--\n⚠️ Failed to flush all messages after %d milliseconds. %d message(s) remain\n", t, r))

		} else {
			if vGeneral.debuglevel >= 1 {
				grpcLog.Infoln("All messages flushed from the queue")
				grpcLog.Infoln("")

			}
		}

		if vGeneral.debuglevel >= 1 {
			vEnd := time.Now()
			grpcLog.Infoln("Start      : ", vStart)
			grpcLog.Infoln("End        : ", vEnd)
			grpcLog.Infoln("Duration   : ", vEnd.Sub(vStart))
			grpcLog.Infoln("Records    : ", vGeneral.testsize)
			grpcLog.Infoln("")
		}

		// --
		// Stop listening to events and close the producer
		// We're ready to finish
		termChan <- true

		// wait for go-routine to terminate
		<-doneChan
		defer p.Close()

	}
	os.Exit(0)

}

func main() {

	pMux := http.NewServeMux()
	promHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	pMux.Handle("/metrics", promHandler)

	// Initialize Prometheus handler
	grpcLog.Info("**** Starting ****")

	go runEFTLoader()

	go func() {
		fmt.Println(http.ListenAndServe(":9000", pMux))
	}()
	select {}
}
