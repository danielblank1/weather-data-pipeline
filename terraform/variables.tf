variable "postgres_user" {
  description = "PostgreSQL username"
  type        = string
  default     = "weather"
}

variable "postgres_password" {
  description = "PostgreSQL password"
  type        = string
  default     = "weather_pipeline_2026"
  sensitive   = true
}

variable "postgres_db" {
  description = "PostgreSQL database name"
  type        = string
  default     = "weather_warehouse"
}
