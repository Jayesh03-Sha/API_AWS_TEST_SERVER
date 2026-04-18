from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("api_set1", "0008_alter_quote_unique_together"),
    ]

    operations = [
        migrations.CreateModel(
            name="AnoudMakeModelMapping",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("bayanaty_make_id", models.CharField(db_index=True, max_length=32)),
                ("bayanaty_model_id", models.CharField(db_index=True, max_length=32)),
                ("tariff_make_code", models.CharField(max_length=32)),
                ("tariff_model_code", models.CharField(max_length=32)),
                ("make_name", models.CharField(blank=True, max_length=120, null=True)),
                ("model_name", models.CharField(blank=True, max_length=120, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "unique_together": {("bayanaty_make_id", "bayanaty_model_id")},
            },
        ),
        migrations.AddIndex(
            model_name="anoudmakemodelmapping",
            index=models.Index(fields=["bayanaty_make_id", "bayanaty_model_id"], name="api_set1_an_bayana_e90d28_idx"),
        ),
    ]

