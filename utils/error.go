package utils

func Panic(err error) {
	if err != nil {
		panic(any(err))
	}
}

func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
