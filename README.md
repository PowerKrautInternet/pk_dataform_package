# The pk_dataform_package
_A data automation package in dataform, developed by PowerKraut._

Current features:
* Data monitoring
* Freshness alerting
* Declaration handling

### Deze package draait om twee principes:

1. **Het definiëren van bronnen**
2. **Het definiëren van welke query's je wilt gebruiken**

Deze structuur is opgezet om consistent, herbruikbaar en modulair om te gaan met verschillende databronnen en lagen in je datamodel, met gebruik van [Dataform](https://dataform.co/).

---

## 📁 Mappenstructuur

De logica van de query's is gestructureerd in submappen onder `queries/`. Dit helpt om lagen van transformatie en kwaliteitscontroles te scheiden:

```
queries/
├── df_datakwaliteit/         → Queries voor kwaliteitscontroles
├── df_rawdata_views/         → Views direct op ruwe brontabellen
├── df_staging_tables/        → Staging-tabellen met initiële transformaties
├── df_staging_views/         → Complexere views opgebouwd uit staging tables
```

Daarnaast:
- `functions/`: Bevat herbruikbare logica.
- `sources/`: Bevat het overzicht van de gedeclareerde bronnen.

---

## 🔧 Gebruik

Query’s worden ingeladen via de package en vervolgens gepubliceerd via standaard Dataform-methodes. Ook bronnen worden centraal gedeclareerd en beschikbaar gesteld aan de rest van de pipeline. Herbruikbare functies, zoals lookups of mappings, zijn beschikbaar via de `functions` map.