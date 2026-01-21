---
id: post_demo_proposal
name: Post-Demo Proposal
trigger: deal_stage_change
trigger_stage: Demo Presentado
trigger_stage_id: "1049659496"
hubspot_template_id: null
variables:
  - first_name
  - company_name
  - current_month
  - payment_link
  - owner_name
---
Subject: Tu propuesta de LaHaus AI para {{ company_name }}

Hola {{ first_name }},

Gracias por tu tiempo en la demo de hoy. Como conversamos, LaHaus AI puede ayudar a {{ company_name }} a:

1. **Responder leads en <10 segundos** - 24/7, sin perder ninguno
2. **Calificar automaticamente** - Solo recibes leads listos para agendar
3. **Aumentar conversiones** - Nuestros clientes ven +35% en citas agendadas

**Tu propuesta personalizada:**

| Servicio | Descripcion | Precio |
|----------|-------------|--------|
| **Asistente de AI** | Respuesta en segundos, calificacion y nutricion de tus leads | $250/mes |
| **Administracion de campañas** (opcional) | Configuramos y optimizamos tus campañas con el Asistente de AI | $250/mes |
| **Setup fee** | Configuracion inicial (unica vez) | $1,000 |

*Como cortesia, el setup fee no se cobrara si activas tu suscripcion durante {{ current_month }}.*

El siguiente paso es activar tu suscripcion a traves de este enlace: {{ payment_link }}

No dudes en escribirme o agendar una llamada para resolver cualquier duda. Puedes responder este correo o agendar directamente en mi calendario.

Quedo atento,

{{ owner_name }}
LaHaus AI
