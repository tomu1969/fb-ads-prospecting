---
id: followup_day14
name: Follow-up Day 14 - Breakup
trigger: delay_after_email
trigger_days: 7
trigger_after: followup_day7
hubspot_template_id: null
variables:
  - first_name
  - company_name
  - owner_name
---
Subject: Cierro tu archivo? {{ first_name }}

Hola {{ first_name }},

He intentado contactarte varias veces sobre LaHaus AI para {{ company_name }}, pero no he recibido respuesta.

Entiendo que puede que:
1. No sea el momento adecuado
2. Ya encontraste otra solucion
3. Simplemente no es prioridad ahora

Cualquiera que sea el caso, esta bien. Voy a cerrar tu archivo por ahora.

Si en algun momento quieres retomar la conversacion, solo responde a este correo y con gusto te ayudo.

Te deseo mucho exito,

{{ owner_name }}
LaHaus AI

P.D. Si hay algo que no te convencio de la propuesta, me encantaria saberlo para mejorar. Tu feedback es valioso.
