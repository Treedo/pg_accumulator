"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.generalLedger = exports.inventory = void 0;
const prisma_accumulator_1 = require("prisma-accumulator");
/**
 * Warehouse inventory register — tracks stock quantity and value
 * by warehouse and product.
 */
exports.inventory = (0, prisma_accumulator_1.defineRegister)({
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
/**
 * General Ledger bookkeeping register — tracks debits/credits
 * by accounts with dynamic subcontingent JSON arrays.
 */
exports.generalLedger = (0, prisma_accumulator_1.defineRegister)({
    name: 'general_ledger',
    kind: 'ledger',
    dimensions: {
        currency: 'text',
    },
    resources: {
        amount: 'numeric(18,2)',
    },
    totals_period: 'day',
});
