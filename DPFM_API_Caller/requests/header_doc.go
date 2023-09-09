package requests

type HeaderDoc struct {
	ProductionOrder		 	 int	`json:"ProductionOrder"`
	ProductionOrderItem		 int	`json:"ProductionOrderItem"`
	Operations		 	 	 int	`json:"Operations"`
	OperationsItem		 	 int	`json:"OperationsItem"`
	OperationID		 	 	 int	`json:"OperationID"`
	ConfirmationCountingID 	 int	`json:"ConfirmationCountingID"`
	DocType                  string `json:"DocType"`
	DocVersionID             int    `json:"DocVersionID"`
	DocID                    string `json:"DocID"`
	FileExtension            string `json:"FileExtension"`
	FileName                 string `json:"FileName"`
	FilePath                 string `json:"FilePath"`
	DocIssuerBusinessPartner int    `json:"DocIssuerBusinessPartner"`
}
