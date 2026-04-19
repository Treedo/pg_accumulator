import { defineRegister } from 'prisma-accumulator';

/**
 * Warehouse inventory register — tracks stock quantity and value
 * by warehouse and product.
 */
export const inventory = defineRegister({
  name: 'inventory',
  kind: 'balance',
  dimensions: {
    warehouse_id: 'int',
    product_id: 'int',
  },
  resources: {
    quantity: 'numeric',
    cost: 'numeric(14,2)',
  },
  totals_period: 'month',
});
