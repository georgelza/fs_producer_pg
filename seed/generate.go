package seed

func GetTenants() map[int]string {

	ar := make(map[int]string)

	ar[1] = "Standard Bank"
	ar[2] = "ABSA"
	ar[3] = "Nedbank"
	ar[4] = "Capitect Bank Limited"
	ar[5] = "Bank Zero"
	ar[6] = "Thyme Bank"
	ar[7] = "FirstRand Limited"
	ar[8] = "Discovery Bank Limited"
	ar[9] = "African Bank Limited"
	ar[10] = "Bidvest"
	ar[11] = "Grindrod"
	ar[12] = "Investec"
	ar[13] = "Ithala"
	ar[14] = "Sasfin Bank Limited"
	ar[15] = "Ubank"

	return ar
}

func GetTransType() map[int]string {

	ar := make(map[int]string)

	ar[1] = "EFT"
	ar[2] = "AC Collection"
	ar[3] = "Payshap"
	ar[4] = "RTC"

	return ar
}

func GetDirection() map[int]string {

	ar := make(map[int]string)

	ar[1] = "Inbound"
	ar[2] = "Outbound"

	return ar
}

func GetEntityId() map[int]string {

	ar := make(map[int]string)

	ar[1] = "Pick n Pay"
	ar[2] = "Spar"
	ar[3] = "Checkers"
	ar[4] = "Billabong"
	ar[5] = "Nike"
	ar[6] = "iStore"
	ar[7] = "Sportsmans Warehouse"
	ar[8] = "Builders"
	ar[9] = "BUCO"
	ar[10] = "ACDC"
	ar[11] = "Spur"
	ar[12] = "McDonals"
	ar[13] = "Kentucky Chicken"
	ar[14] = "Discem"
	ar[15] = "Clicks"
	ar[16] = "Edgars"
	ar[17] = "Truworths"
	ar[18] = "Markhams"
	ar[19] = "CNA"
	ar[20] = "MrPrice"
	ar[21] = "Ackermans"
	ar[22] = "Starbucks"
	ar[23] = "Incredible Connection"
	ar[24] = "CycleLab"
	ar[25] = "Steers"
	ar[26] = "World of Golf"

	return ar
}

func GetRiskStatus() map[int]string {

	ar := make(map[int]string)

	ar[1] = "reviewe"
	ar[2] = "risk-noreview"
	ar[3] = "no-risk"

	return ar
}
