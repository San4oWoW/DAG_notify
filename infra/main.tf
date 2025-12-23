resource "terraform_data" "stack" {
  triggers_replace = {
    project_root = abspath(var.project_root)
    compose_file = var.compose_file
    compose_hash = filesha256("${var.project_root}/${var.compose_file}")
  }

  provisioner "local-exec" {
    working_dir = self.triggers_replace.project_root
    command     = "docker compose -f ${self.triggers_replace.compose_file} up -d"
  }

  provisioner "local-exec" {
    when        = destroy
    working_dir = self.triggers_replace.project_root
    command     = "docker compose -f ${self.triggers_replace.compose_file} down"
  }
}
