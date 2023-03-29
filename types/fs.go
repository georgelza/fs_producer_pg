package types

// My fake data structures - GoFakeIt
type TPaddress struct {
	Streetname string  `json:streetname`
	City       string  `json:city`
	State      string  `json:state`
	Zip        string  `json:zip`
	Country    string  `json:country`
	Latitude   float64 `json:latitude`
	Longitude  float64 `json:longitude`
}

type TPccard struct {
	Type   string `json:"type"`
	Number int    `json:"number"`
	Exp    string `json:"exp"`
	Cvv    string `json:"cvv"`
}

type TPjobinfo struct {
	Company    string `json:"company"`
	Title      string `json:"title"`
	Descriptor string `json:"descriptor"`
	Level      string `json:"level"`
}

type TPcontact struct {
	Email string `json:"email"`
	Phone string `json:"phone"`
}

type TPperson struct {
	Ssn          string    `json:"ssn"`
	Firstname    string    `json:"firstname"`
	Lastname     string    `json:"lastname"`
	Gender       string    `json:"gender"`
	Address      TPaddress `json:"address"`
	Contact      TPcontact `json:"contact"`
	Ccard        TPccard   `json:"ccard"`
	Job          TPjobinfo `json:"job"`
	Created_date string    `json:"created_date"`
}

// FS engineResponse components
type TPamount struct {
	BaseCurrency string `json:"basecurrency,omitempty"`
	BaseValue    string `json:"basevalu,omitempty"`
	Burrency     string `json:"currency,omitempty"`
	Value        string `json:"value,omitempty"`
}

type TPtags struct {
	Tag    string   `json:"tag,omitempty"`
	Values []string `json:"values,omitempty"`
}

type TPmodelData struct {
	Adata string `json:"adata,omitempty"`
}

type TPmodels struct {
	ModelId    string      `json:"modelid,omitempty"`
	Score      float64     `json:"score,omitempty"`
	Confidence float64     `json:"confidence,omitempty"`
	Tags       []TPtags    `json:"tags,omitempty"`
	ModelData  TPmodelData `json:"modeldata,omitempty"`
}

type TPrules struct {
	RuleId string  `json:"ruleid,omitempty"`
	Score  float64 `json:"score,omitempty"`
}

type TPscore struct {
	Models []TPmodels `json:"models,omitempty"`
	Tags   []TPtags   `json:"tags,omitempty"`
	Rules  []TPrules  `json:"rules,omitempty"`
}

type TPaggregator struct {
	AggregatorId   string   `json:"aggregatorId,omitempty"`
	Scores         TPscore  `json:"scores,omitempty"`
	AggregateScore float64  `json:"aggregateScore,omitempty"`
	MatchedBound   float64  `json:"matchedBound,omitempty"`
	OutputTags     []TPtags `json:"outputTags,omitempty"`
	SuppressedTags []TPtags `json:"suppressedTags,omitempty"`
	Alert          bool     `json:"alert,omitempty"`
	SuppressAlert  bool     `json:"suppressAlert,omitempty"`
}

type TPconfigGroups struct {
	Type           string         `json:"type,omitempty"`
	Version        string         `json:"version,omitempty"`
	Id             string         `json:"id,omitempty"`
	TriggeredRules []string       `json:"triggeredRules,omitempty"`
	Aggregators    []TPaggregator `json:"aggregators,omitempty"`
}

type TPversions struct {
	ModelGraph   int              `json:"modelgraph,omitempty"`
	ConfigGroups []TPconfigGroups `json:"configGroups,omitempty"`
}

type TPoverallScore struct {
	AggregationModel string  `json:"aggregationModel,omitempty"`
	OverallScore     float64 `json:"overallScore,omitempty"`
}

type TPentity struct {
	TenantId     string           `json:"tenantId,omitempty"`
	EntityType   string           `json:"entityType,omitempty"`
	EntityId     string           `json:"entityId,omitempty"`
	OverallScore TPoverallScore   `json:"overallScore,omitempty"`
	Models       []TPmodels       `json:"models,omitempty"`
	FailedModels []string         `json:"failedModels,omitempty"`
	OutputTags   []TPtags         `json:"outputTags,omitempty"`
	RiskStatus   string           `json:"riskStatus,omitempty"`
	ConfigGroups []TPconfigGroups `json:"configGroups,omitempty"`
}

type TPengineResponse struct {
	Entities         []TPentity             `json:"entities,omitempty"`
	JsonVersion      int                    `json:"jsonVersion,omitempty"`
	OriginatingEvent map[string]interface{} `json:"originatingEvent,omitempty"`
	OutputTime       string                 `json:"outputTime,omitempty"`
	ProcessorId      string                 `json:"processorId,omitempty"`
	Versions         TPversions             `json:"versions,omitempty"`
}
