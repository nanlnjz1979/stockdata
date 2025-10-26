from django.contrib import admin
from django.urls import path, include
from django.http import HttpResponse, JsonResponse


def home_view(request):
    return HttpResponse("Stock Data Server is running. Visit /api/ or /healthz")


def healthz(request):
    return JsonResponse({"status": "ok"})

urlpatterns = [
    path('', home_view),
    path('healthz', healthz),
    path('admin/', admin.site.urls),
    path('api/', include('stocks.urls')),
]