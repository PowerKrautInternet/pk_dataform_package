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

# 🔧 In gebruik nemen

Query’s worden ingeladen via de package en vervolgens gepubliceerd via standaard Dataform-methodes. Ook bronnen worden centraal gedeclareerd en beschikbaar gesteld aan de rest van de pipeline. Herbruikbare functies, zoals lookups of mappings, zijn beschikbaar via de `functions` map.

## Stap 1: Declarations.js
Dit is het bestand dat altijd als eerste uitgevoerd zal worden in uw dataform omgeving. Hier zullen we dan ook alle standaard query's opzetten en onze inkomende bronnen defineren.

Het grootste verschil met de standaard format die dataform aanneemt is dat wij de bronnen eerst verzamelen voordat we ze in een declare functie aanroepen, hierdoor bestaat declerations.js nu uit 3 onderdelen, waarbij je eigelijk alleen de 'const sources' hoeft aan te passen:

```declerations.js
const sources = [
  {
    // De normale rijen die nodig zijn voor een declare vul je hier ook in. Zoals; database, schema en name.
    database: "dtc-datalake-prod",
    schema: "df_staging_views",
    name: "stg_openRdwData",
    alias: "GA4", // Hier vul je een naam in die meerdere bronnen kan groepen. In dit voorbeeld zou je bijvoorbeeld meerdere bronnen als GA4 kunnen defineren en zo altijd gezamelijk kunnen aanroepen in een ref()
    crm_id: "982", // Hiermee kan je aangeven op welk CRM_ID gefilterd moet worden. (BETA: nog niet overal geimplementeerd)
  },
  {
    schema: ...
    name: ...
    ...
  }
]

for (let s in sources) {
    declare(sources[s]);
    if(typeof sources[s].name != "undefined" && sources[s].name.endsWith("DataProducer")){declare({schema: "df_rawdata_views", name: sources[s].name+"_lasttransaction"})}
};
require("pk_dataform_package/sources").setSources(sources);
operate("setup_operations", require("pk_dataform_package/setup").setupFunctions(sources))
```
!! Let op: als deze stap word toegevoegd zullen ook automatisch alle "_lasttransation" schema's toegevoegd worden! Dit kan dus voor dubbele schema's zorgen.

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
- `setup/`: Bevat standaard functionaliteiten die dmv een operation geimplemeerd kan worden.
  - Elke dataproducer word automatisch een `_lasttransaction` view voor aangemaakt.
- `sources/`: Bevat het overzicht van de gedeclareerde bronnen.

**CRM_ID defineren voor een bron**

* Bij het linken van een CRM_ID aan een bron is de alias gelinkt aan de CRM_ID. Dat doe je bijvoorbeeld zo:
`{ alias: "syntec", crm_id: "982" }`

**Velden die dynamisch worden toegevoegd op basis van beschikbaarheid**

```
stg_activecampaign_ga4_sheets/
├── gs_activecampaign_ga4_mapping/
```