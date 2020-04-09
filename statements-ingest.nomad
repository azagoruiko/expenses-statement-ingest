job "statements-ingest" {
  datacenters = ["home"]
  type        = "batch"

  periodic {
    cron      = "10 20 * * * *"
    prohibit_overlap = true
  }

  group "statements-ingest-group" {
    count = 1
    task "statements-ingest-task" {
      driver = "docker"
      template {
        data = <<EOH
POSTGRES_JDBC_URL="{{ key "postgres.jdbc.url" }}"
POSTGRES_JDBC_DRIVER="{{ key "postgres.jdbc.driver" }}"
POSTGRES_JDBC_USER="{{ key "postgres.jdbc.user" }}"
POSTGRES_JDBC_PASSWORD="{{ key "postgres.jdbc.password" }}"
EOH
        destination = "secrets.env"
        env = true
      }

      config {
        network_mode = "host"
        extra_hosts = ["nuc1:192.168.0.21", "nuc2:192.168.0.22", "nuc3:192.168.0.23", "master:192.168.0.10", "slave0:192.168.0.11"]
        privileged = true
        image = "127.0.0.1:9999/docker/expenses-statement-ingest:1.0.7"
        command = "bash"
        args = [
          "/app/run.sh",
          "1.0.7",
        ]
      }

      resources {
        cpu    = 1000
        memory = 1024

        network {
          mbits = 20
        }
      }
    }
  }
}
