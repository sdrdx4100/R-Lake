from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from .models import Dataset
from .serializers import DatasetSerializer
import plotly.express as px

class DatasetViewSet(viewsets.ModelViewSet):
    queryset = Dataset.objects.all().order_by('-uploaded_at')
    serializer_class = DatasetSerializer

    @action(detail=True, methods=['get'])
    def plot(self, request, pk=None):
        ds = self.get_object()
        x_col = request.query_params.get('x')
        y_col = request.query_params.get('y')
        if not x_col or not y_col:
            return Response({'detail': 'x,y parameters required'}, status=status.HTTP_400_BAD_REQUEST)
        fig = px.line(ds.data, x=x_col, y=y_col, title=f"{ds.name}: {y_col} vs {x_col}")
        return Response(fig.to_dict())

