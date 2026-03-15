package config

type Stock struct {
	Code string
	Name string
}

var KoreaStocks = []Stock{
	{Code: "005930", Name: "삼성전자"},
	{Code: "000660", Name: "SK하이닉스"},
	{Code: "373220", Name: "LG에너지솔루션"},
	{Code: "207940", Name: "삼성바이오로직스"},
	{Code: "005380", Name: "현대차"},
	{Code: "000270", Name: "기아"},
	{Code: "068270", Name: "셀트리온"},
	{Code: "005490", Name: "POSCO홀딩스"},
	{Code: "035420", Name: "NAVER"},
	{Code: "003670", Name: "포스코퓨처엠"},
	{Code: "051910", Name: "LG화학"},
	{Code: "028260", Name: "삼성물산"},
	{Code: "032830", Name: "삼성생명"},
	{Code: "105560", Name: "KB금융"},
	{Code: "012330", Name: "현대모비스"},
	{Code: "035720", Name: "카카오"},
	{Code: "055550", Name: "신한지주"},
	{Code: "066570", Name: "LG전자"},
	{Code: "000810", Name: "삼성화재"},
	{Code: "033780", Name: "KT&G"},
	{Code: "015760", Name: "한국전력"},
	{Code: "034730", Name: "SK"},
	{Code: "034020", Name: "두산에너빌리티"},
	{Code: "011200", Name: "HMM"},
	{Code: "051900", Name: "LG생활건강"},
	{Code: "086520", Name: "에코프로"},
	{Code: "247540", Name: "에코프로비엠"},
	{Code: "323410", Name: "카카오뱅크"},
	{Code: "316140", Name: "우리금융지주"},
	{Code: "010130", Name: "고려아연"},
	{Code: "017670", Name: "SK텔레콤"},
	{Code: "009150", Name: "삼성전기"},
	{Code: "042700", Name: "한미반도체"},
	{Code: "003550", Name: "LG"},
	{Code: "005830", Name: "DB손해보험"},
	{Code: "086790", Name: "하나금융지주"},
	{Code: "018260", Name: "삼성SDS"},
	{Code: "024110", Name: "기업은행"},
	{Code: "090430", Name: "아모레퍼시픽"},
	{Code: "004020", Name: "현대제철"},
}

func WatchList(n int) []string {
	stocks := KoreaStocks
	if n > 0 && n < len(stocks) {
		stocks = stocks[:n]
	}
	codes := make([]string, len(stocks))
	for i, s := range stocks {
		codes[i] = s.Code
	}
	return codes
}
