from django.conf.urls import patterns, include, url

from service import views
# from settings import MANAGE_OPEN

MANAGE = False

urlpatterns = patterns('')
if MANAGE:
    urlpatterns += patterns('',
                            url(r'^manage/', include('manage.urls')))

urlpatterns += patterns('',
                        # url(r'^manage/', include('manage.urls')),
                        url('[\\d\\D]+', views.FacadeView.as_view()))



