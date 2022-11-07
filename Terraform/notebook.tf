variable "notebook_subdirectory" {
  type = string
}

variable "notebook_filename" {
  type = string
}

variable "notebook_language" {
  type = string
}

resource "databricks_notebook" "this" {
  path = "${data.databricks_current_user.me.home}/${var.notebook_subdirectory}/${var.notebook_filename}"
  language = var.notebook_language
  source = "./../${var.notebook_filename}"
}

output "notebook_url" {
  value = databricks_notebook.this.url
}
