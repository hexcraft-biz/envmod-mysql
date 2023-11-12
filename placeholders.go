package mysql

func PositionalPlaceholders(num int) []string {
	phs := make([]string, num)
	for i := range phs {
		phs[i] = "?"
	}
	return phs
}
