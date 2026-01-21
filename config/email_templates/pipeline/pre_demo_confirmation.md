---
id: pre_demo_confirmation
name: Pre-Demo Confirmation
trigger: deal_stage_change
trigger_stage: Demo Agendado
trigger_stage_id: "1049659495"
hubspot_template_id: null
variables:
  - first_name
  - company_name
  - demo_date
  - demo_time
  - meeting_link
  - owner_name
---
Subject: Confirmado: Tu demo de LaHaus AI el {{ demo_date }}

Hola {{ first_name }},

Tu demo esta confirmada para el **{{ demo_date }}** a las **{{ demo_time }}**.

**Link de la reunion:** {{ meeting_link }}

En la demo vamos a:
- Ver como LaHaus AI responde leads en menos de 10 segundos
- Revisar tu flujo actual y donde se pierden leads
- Mostrar casos de exito en equipos similares al tuyo 

Si necesitas reagendar, avisame con 24h de anticipacion.

Nos vemos pronto,

{{ owner_name }}
LaHaus AI
