"""
URL configuration for sl_server project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
import config_manager.views as views


urlpatterns = [
    path('', views.job_page, name='job_page'),
    path('job-page/', views.job_page, name='job_page'),
    path('upload-config/', views.upload_config, name='upload_config'),
    path('run-config/<str:config_name>/', views.run_config, name='run_config'),
    path('log-viewer/', views.log_viewer, name='log_viewer'),
    path('view-log/<str:log_name>/', views.view_log, name='view_log'),
    path('delete-log/<str:log_name>/', views.delete_log, name='delete_log'),
    path('report-pdf-viewer/', views.report_pdf_viewer, name='report_pdf_viewer'),
    path('view-report/<str:report_name>/', views.view_report, name='view_report'),
    #    path('download/<str:report_name>/', views.download_report, name='download_report'),
]

