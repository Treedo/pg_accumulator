/**
 * Warehouse inventory register — tracks stock quantity and value
 * by warehouse and product.
 */
export declare const inventory: import("prisma-accumulator").Register<{
    warehouse_id: string;
    product_id: string;
}, {
    quantity: string;
    cost: string;
}>;
/**
 * General Ledger bookkeeping register — tracks debits/credits
 * by accounts with dynamic subcontingent JSON arrays.
 */
export declare const generalLedger: import("prisma-accumulator").Register<{
    currency: string;
}, {
    amount: string;
}>;
