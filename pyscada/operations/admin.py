from django.contrib import admin

from pyscada.admin import admin_site

from .models import PeriodicField


class PeriodicFieldAdmin(admin.ModelAdmin):
    def has_module_permission(self, request):
        return False


admin_site.register(PeriodicField, PeriodicFieldAdmin)
