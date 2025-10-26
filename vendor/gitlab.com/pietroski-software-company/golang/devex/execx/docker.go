package execx

func DockerComposeUp(dockerfile string) error {
	return Run("docker", "compose", "-f", dockerfile, "up", "-d", "--build", "--remove-orphans")
}

func DockerComposeDown(dockerfile string) error {
	return Run("docker", "compose", "-f", dockerfile, "down")
}
