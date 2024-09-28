import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand

from archiv.models import (
    ArtWork,
    Person,
    Institution,
    Book,
    Text,
)


class Command(BaseCommand):
    help = "Import Data"

    def handle(self, *args, **kwargs):
        dbc = settings.LEGACY_DB_CONNECTION
        db_connection_str = f"mysql+pymysql://{dbc['USER']}:{dbc['PASSWORD']}@{dbc['HOST']}/{dbc['NAME']}"
        db_connection = create_engine(db_connection_str)

        source_name = "writes"
        query = f"SELECT * FROM {source_name}"
        print(query)
        df = pd.read_sql(query, con=db_connection)
        for i, row in tqdm(df.iterrows(), total=len(df)):
            person = Person.objects.get(legacy_pk=row["pid"])
            book = Book.objects.get(legacy_pk=row["bid"])
            person.author_of.add(book)

        source_name = "paints"
        query = f"SELECT * FROM {source_name}"
        print(query)
        df = pd.read_sql(query, con=db_connection)
        for i, row in tqdm(df.iterrows(), total=len(df)):
            person = Person.objects.get(legacy_pk=row["pid"])
            book = ArtWork.objects.get(legacy_pk=row["aid"])
            person.painter_of.add(book)

        source_name = "referencesart"
        query = f"SELECT * FROM {source_name}"
        print(query)
        df = pd.read_sql(query, con=db_connection)
        for i, row in tqdm(df.iterrows(), total=len(df)):
            try:
                text = Text.objects.get(legacy_pk=row["tid"])
            except ObjectDoesNotExist:
                continue
            try:
                artwork = ArtWork.objects.get(legacy_pk=row["aid"])
            except ObjectDoesNotExist:
                continue
            text.mentioned_artwork.add(artwork)

        source_name = "referencespeople"
        query = f"SELECT * FROM {source_name}"
        print(query)
        df = pd.read_sql(query, con=db_connection)
        for i, row in tqdm(df.iterrows(), total=len(df)):
            try:
                text = Text.objects.get(legacy_pk=row["tid"])
            except ObjectDoesNotExist:
                continue
            try:
                person = Person.objects.get(legacy_pk=row["pid"])
            except ObjectDoesNotExist:
                continue
            text.mentioned_person.add(person)

        print("fix names for institutions")
        for x in tqdm(Institution.objects.all()):
            x.name = x.legacy_id
            x.save()
        print("done")
