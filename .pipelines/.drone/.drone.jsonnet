local FromSecret(secret) = {
	"from_secret": secret,
};

local envs() = {
	GITLAB_ACCESS_TOKEN: FromSecret("GITLAB_ACCESS_TOKEN"),
	GITLAB_USERNAME: FromSecret("GITLAB_USERNAME"),
};

local remote_git_repo_address = 'https://gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node.git';

local set_netrc = [
	"apk update && apk upgrade && apk add git bash make build-base ncurses",
	'echo -e "machine gitlab.com\nlogin $${GITLAB_USERNAME}\npassword $${GITLAB_ACCESS_TOKEN}" > ~/.netrc',
	"chmod 640 ~/.netrc",

	'echo -e "machine gitlab.com\nlogin $${GITLAB_USERNAME}\npassword $${GITLAB_ACCESS_TOKEN}" > build/docker/.netrc',
	"chmod 640 build/docker/.netrc",
];

local set_ptk_envs = [
	"go env -w GONOSUMDB=gitlab.com/pietroski-software-company",
	"go env -w GONOPROXY=gitlab.com/pietroski-software-company",
	"go env -w GOPRIVATE=gitlab.com/pietroski-software-company",

	'git config --global user.email "pietroskisoftwarecompany@gmail.com"',
  'git config --global user.name "ptk.swe"',
];

local dependencies = std.flattenArrays([
	set_netrc,
	set_ptk_envs,
]);

local dockerSocketVolume() = {
	name: "dockersock",
  path: "/var/run/docker.sock",
};

local unit_tests_cmd = [
	"go test -race $(go list ./... | grep -v /integration/ | grep -v /tests/ | grep -v /mocks/ | grep -v /schemas/ | grep -v /benchmark/)",
];

local integraiton_tests_cmd = [
	"make docker-compose-up-tests-integration-ltng-db",
	"go clean -testcache",
	"export $(grep -v '^#' ./tests/integration/lightning-db/.tests.integration.ltng.db.env | xargs)",
	"go test -race ./tests/integration/...",
	"make docker-compose-down-tests-integration-ltng-db",
];

local unit_tests_suite_cmd = std.flattenArrays([
	dependencies,
	unit_tests_cmd,
]);

local unit_tests_suite(name, image, envs) = {
	name: name,
  image: image,
  environment: envs,
  commands: unit_tests_suite_cmd,
};

local integration_tests_suite_cmd = std.flattenArrays([
	dependencies,
  integraiton_tests_cmd,
]);

local integration_tests_suite(name, image, envs) = {
	name: name,
  image: image,
  privileged: true,
  network_mode: "host",
  environment: envs,
  commands: integration_tests_suite_cmd,
  volumes: [
    dockerSocketVolume(),
  ],
};

local gitlab_push = [
	"git checkout -b release/merging-branch",
  "git remote add gitlab "+remote_git_repo_address,
  "git push gitlab release/merging-branch -f",
];

local gitlabPushStep(image, envs, dependency_list) = {
	name: "gitlab-push",
  image: image,
  environment: envs,
  commands: std.flattenArrays([set_netrc, gitlab_push]),
  depends_on: dependency_list,
};

local gitlab_tag = [
  "git remote add gitlab "+remote_git_repo_address,
  "make tag",
  "git push gitlab --tags",
];

local gitlabTagStep(image, envs, dependency_list) = {
	name: "gitlab-tag",
  image: image,
  environment: envs,
  commands: std.flattenArrays([set_netrc, gitlab_tag]),
  depends_on: dependency_list
};

local whenCommitToNonMaster(step) = step {
  when: {
    event: ['push'],
    branch: {
      exclude: ['main'],
    },
  },
};

local commitToNonMasterSteps(image, envs) = std.map(whenCommitToNonMaster, [
	unit_tests_suite("unit-tests-suite-non-master", image, envs),
	integration_tests_suite("integration-tests-suite-non-master", image, envs),
]);

local whenCommitToMaster(step) = step {
	when: {
		event: ['push'],
		branch: ['main'],
	},
};

local commitToMasterSteps(image, envs) = std.map(whenCommitToMaster, [
	unit_tests_suite("unit-tests-suite-master", image, envs),
	integration_tests_suite("integration-tests-suite-master", image, envs),
	gitlabPushStep(image, envs, [
    'unit-tests-suite-master',
    'integration-tests-suite-master',
  ]),
]);

local whenTag(step) = step {
	when: {
		event: ['tag'],
		branch: ['*'],
	},
};

local tagSteps(image, envs) = std.map(whenTag, [
	unit_tests_suite("unit-tests-suite-tagging", image, envs),
	integration_tests_suite("integration-tests-suite-tagging", image, envs),
	gitlabTagStep(image, envs, [
    'unit-tests-suite-tagging',
    'integration-tests-suite-tagging',
  ]),
  // deploy / push docker image
	// deploy to kluster event
]);

local dockerSocketVolumeSetup() = {
	name: "dockersock",
  tmp: {},
  host: {
    path: "/var/run/docker.sock"
  },
};

local Pipeline(name, image) = {
  kind: "pipeline",
  name: name,
  steps: std.flattenArrays([
    commitToNonMasterSteps(image, envs()),
    commitToMasterSteps(image, envs()),
    tagSteps(image, envs()),
  ]),
  volumes: [
    dockerSocketVolumeSetup(),
  ],
};

[
  Pipeline("lightning-node-pipeline", "pietroski/alpine-docker-golang:v0.0.2"), // pietroski/alpine-docker-golang:v0.0.1 - golang:1.21.3-alpine3.18
]
