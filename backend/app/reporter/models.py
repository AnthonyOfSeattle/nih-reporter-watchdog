from django.db import models
from django.utils.translation import gettext_lazy as _


class ProjectRecord(models.Model):
    class Meta:
        indexes = [
            models.Index(fields=["appl_id"])
        ]

    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    appl_id = models.IntegerField()
    project_num = models.CharField(max_length=64)
    fain = models.CharField(max_length=11, null=True)

    date_of_last_change = models.DateTimeField(null=True)
    data_hash = models.TextField()
    data = models.JSONField()


class ProjectRecordChange(models.Model):
    class Flag(models.TextChoices):
        ALTERED = "ALT", _("Altered")
        REDUCED = "RED", _("Reduced")
        INCREASED = "INC", _("Increased")
        DELETED = "DEL", _("Deleted")

    class ValueType(models.TextChoices):
        INTEGER = "INT", _("Integer")
        FLOAT = "FLOAT", _("Float")
        STRING = "STR", _("String")
        DATETIME = "DT", _("DateTime")

    project_record = models.ForeignKey(
        ProjectRecord,
        on_delete=models.CASCADE
    )
    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    date_of_change = models.DateTimeField()
    flag = models.CharField(max_length=3,choices=Flag)
    field = models.TextField(null=True)
    data = models.JSONField(null=True)
