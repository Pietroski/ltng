package v1

import "time"

func timeNowUnixUTC() int64 {
	return time.Now().UTC().Unix()
}
