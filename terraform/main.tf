# ============================================================
# Terraform configuration for local Docker infrastructure
# Manages Docker networks and volumes declaratively
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

resource "docker_network" "weather_net" {
  name   = "weather_net"
  driver = "bridge"
}

# ---------------------------------------------------------------------------
# Persistent volumes
# ---------------------------------------------------------------------------

resource "docker_volume" "postgres_data" {
  name = "weather_postgres_data"
}

resource "docker_volume" "kestra_data" {
  name = "weather_kestra_data"
}

resource "docker_volume" "spark_data" {
  name = "weather_spark_data"
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "network_id" {
  value = docker_network.weather_net.id
}

output "postgres_volume" {
  value = docker_volume.postgres_data.name
}

output "kestra_volume" {
  value = docker_volume.kestra_data.name
}
