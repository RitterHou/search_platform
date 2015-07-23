from django.conf.urls import patterns, include, url

from service import views
# from settings import MANAGE_OPEN

MANAGE = True

urlpatterns = patterns('')
if MANAGE:
    urlpatterns += patterns('',
                            url(r'^management/', include('manage.urls')))

urlpatterns += patterns('',
                        url('[\\d\\D]+', views.RestfulFacadeView.as_view()))



