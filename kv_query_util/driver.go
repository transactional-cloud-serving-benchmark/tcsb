package kv_query_util

type Driver interface {
	Setup()
	ExecuteOnce(*RequestResponse)
	Teardown()
}
