# DIC (and others) can return multiple schemes/plans per quote request with the same provider name.
# The previous (quote_request, provider) unique pair blocked saving more than one plan.
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("api_set1", "0007_quote_provider_metadata_and_more"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="quote",
            unique_together=(),
        ),
    ]
