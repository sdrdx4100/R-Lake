from django.db import models
from django.contrib.postgres.fields import JSONField  # PostgreSQL を想定

class Dataset(models.Model):
    name = models.CharField(max_length=200)
    uploaded_at = models.DateTimeField(auto_now_add=True)
    data = JSONField()  # CSV を一行ずつ JSON 配列で保存

    def __str__(self):
        return self.name

