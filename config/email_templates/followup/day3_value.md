---
id: followup_day3
name: Follow-up Day 3 - Value Reminder
trigger: delay_after_email
trigger_days: 3
trigger_after: post_demo_proposal
hubspot_template_id: null
variables:
  - first_name
  - company_name
  - owner_name
---
Subject: Pregunta rapida sobre la propuesta, {{ first_name }}

Hola {{ first_name }},

Queria hacer seguimiento a la propuesta que te envie para {{ company_name }}.

Entiendo que estas ocupado, pero queria recordarte el costo de no actuar:

- Cada lead que no se responde en 5 minutos tiene 80% menos probabilidad de convertir
- Los fines de semana y noches se pierden oportunidades valiosas
- Tu equipo de ventas gasta tiempo calificando leads frios

LaHaus AI resuelve esto desde el dia 1.

Tienes alguna pregunta sobre la propuesta? Estoy disponible para una llamada rapida cuando te funcione.

{{ owner_name }}
LaHaus AI
