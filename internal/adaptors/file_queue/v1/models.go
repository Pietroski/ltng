package filequeuev1

const (
	ltngFileQueueBasePath = ".ltngfq" // ".ltng_file_queue"
	fileQueueKey          = "file-queue"
	sep                   = "/"
	fqPrefix              = "fq"
	ext                   = ".ptk"
	tmpPrefix             = "tmp-"
	fqFile                = fqPrefix + ext
	fqTmpFile             = tmpPrefix + fqPrefix + ext

	createPrefix = "create-"
	upsertPrefix = "upsert-"
	deletePrefix = "delete-"

	CreateFileQueueFileName = createPrefix + fqPrefix + ext
	UpsertFileQueueFileName = upsertPrefix + fqPrefix + ext
	DeleteFileQueueFileName = deletePrefix + fqPrefix + ext

	CreateFileQueueFilePath = "action-queue/create"
	UpsertFileQueueFilePath = "action-queue/upsert"
	DeleteFileQueueFilePath = "action-queue/delete"

	genericPrefix            = "generic-"
	GenericFileQueueFileName = genericPrefix + fqPrefix // + ext
	GenericFileQueueFilePath = "action-queue/generic"

	dbFilePerm = 0750
)

func getFilePath(path, filename string) string {
	return ltngFileQueueBasePath + sep + path + sep + filename + ext
}

func getTmpFilePath(path, filename string) string {
	return ltngFileQueueBasePath + sep + path + sep + tmpPrefix + filename + ext
}
