import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import QuarantineBatchView from '@/views/QuarantineBatchView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    {
      path: '/quarantine_zone/batch/:lakeTable/:batchId',
      name: 'quarantine-batch',
      component: QuarantineBatchView,
      props: true,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
