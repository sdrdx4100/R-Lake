from rest_framework import serializers
from .models import Dataset
import csv, io

class DatasetSerializer(serializers.ModelSerializer):
    file = serializers.FileField(write_only=True)

    class Meta:
        model = Dataset
        fields = ['id', 'name', 'uploaded_at', 'data', 'file']
        read_only_fields = ['id', 'uploaded_at', 'data']

    def create(self, validated_data):
        f = validated_data.pop('file')
        text = io.TextIOWrapper(f, encoding='utf-8')
        reader = csv.DictReader(text)
        rows = list(reader)
        ds = Dataset.objects.create(data=rows, **validated_data)
        return ds

