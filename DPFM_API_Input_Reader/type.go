package dpfm_api_input_reader

type SDC struct {
	ConnectionKey     string    `json:"connection_key"`
	Result            bool      `json:"result"`
	RedisKey          string    `json:"redis_key"`
	Filepath          string    `json:"filepath"`
	APIStatusCode     int       `json:"api_status_code"`
	RuntimeSessionID  string    `json:"runtime_session_id"`
	BusinessPartnerID *int      `json:"business_partner"`
	ServiceLabel      string    `json:"service_label"`
	APIType           string    `json:"api_type"`
	HeaderDoc         HeaderDoc `json:"ProductionOrderConfirmation"`
	APISchema         string    `json:"api_schema"`
	Accepter          []string  `json:"accepter"`
	Deleted           bool      `json:"deleted"`
}

type HeaderDoc struct {
	ProductionOrder		 	 *int	`json:"ProductionOrder"`
	ProductionOrderItem		 *int	`json:"ProductionOrderItem"`
	Operations		 	 	 *int	`json:"Operations"`
	OperationsItem		 	 *int	`json:"OperationsItem"`
	OperationID		 	 	 *int	`json:"OperationID"`
	ConfirmationCountingID 	 *int	`json:"ConfirmationCountingID"`
	DocType                  *string `json:"DocType"`
	DocVersionID             *int    `json:"DocVersionID"`
	DocID                    *string `json:"DocID"`
	FileExtension            *string `json:"FileExtension"`
	FileName                 *string `json:"FileName"`
	FilePath                 *string `json:"FilePath"`
	DocIssuerBusinessPartner *int    `json:"DocIssuerBusinessPartner"`
}
