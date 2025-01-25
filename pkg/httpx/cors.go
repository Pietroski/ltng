package httpx

import "net/http"

func EnableCors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//originHeader := r.Header.Get("Origin")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "OPTIONS,GET,POST,PUT,PATCH,DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept,Origin,Content-Type,Content-Length,Accept-Encoding,X-CSRF-Token,Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		h.ServeHTTP(w, r)
	})
}
