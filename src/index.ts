import { Knex } from 'knex';
import { Callback, Transaction } from '@mmstudio/an000049';
import wrapFilter from '@mmstudio/an000059';

export default function dataWrap<T extends {} = any>(db: Knex<any, unknown[]>, tableName: string) {
	type Data = Partial<T>;
	return {
		table() {
			return db<T>(tableName);
		},
		/**
		 * 新增
		 */
		async insert(data: Data | Data[]) {
			if (Array.isArray(data) && data.length === 0) {
				return;
			}
			await this.table().insert(data as any);
		},
		/**
		 * 修改
		 */
		update(data: Data, filter: Data) {
			return this.table().update(data as any).where(wrapFilter(filter));
		},
		/**
		 * 查询数据总条数
		 */
		async count(callback = ((q) => { return q; }) as Callback<T>) {
			const tb = this.table();
			const qb = (callback(tb) as typeof tb) || tb;
			const [{ size }] = await qb.count('*', { as: 'size' });
			return Number(size);
		},
		/**
		 * 查询列表
		 */
		async list(searchFields: Array<keyof T>, keywords: string, page: string | number, pagesize: string | number, filter = {} as Data, callback = ((q) => { return q.select('*'); }) as Callback<T>, whereCallback = ((q) => { return q; }) as Callback<T>) {
			const size = Number(pagesize);
			const offset = (Number(page) - 1) * size;
			const tb = this.table();
			if (keywords) {
				tb.andWhere((builder) => {
					searchFields.forEach((field) => {
						builder.orWhere(field as string, 'like', `%${keywords}%`);
					});
				});
			}
			const query = wrapFilter(filter);
			const q = tb.where(query);

			if (size > 0) {
				q.limit(size)
					.offset(offset);
			}
			const t = (whereCallback(q) as typeof q) || q;
			const qb = (callback(t) as typeof t) || t;
			const data = (await qb) as T[];
			const total = await this.count((qb) => {
				qb.where(query);
				const t = (whereCallback(qb) as typeof qb) || qb;
				if (!keywords) {
					return t;
				}
				return t.andWhere((builder) => {
					searchFields.forEach((field) => {
						builder.orWhere(field as string, 'like', `%${keywords}%`);
					});
				});
			});
			return {
				data,
				total
			};
		},
		/**
		 * 查询全部列表
		 */
		query(filter = {} as Data) {
			return this.table().where(wrapFilter(filter));
		},
		/**
		 * 查询一个
		 */
		first(filter = {} as Data) {
			return this.table().first('*').where(wrapFilter(filter));
		},
		/**
		 * 删除
		 */
		delete(filter = {} as Data) {
			return this.table().del().where(wrapFilter(filter));
		}
	};
}

export async function dataWrapTrx<T extends {} = any>(db: Knex<any, unknown[]>, tableName: string, trxOrTimeout?: Transaction | number) {
	const trx = await (async () => {
		const timeOut = 5000;
		if (typeof trxOrTimeout === 'undefined' || typeof trxOrTimeout === 'number') {
			const trx = await db.transaction();
			setTimeout(() => {
				if (trx.isCompleted()) {
					return;
				}
				trx.rollback('Time out');
			}, trxOrTimeout || timeOut);
			return trx;
		}
		return trxOrTimeout;
	})();
	return {
		/**
		 * 获取事务句柄，可以供一同的其它查询共用事务
		 */
		getTransaction() {
			return trx;
		},
		/**
		 * 提交
		 */
		async commit() {
			if (trx.isCompleted()) {
				return;
			}
			await trx.commit();
		},
		/**
		 * 撤消修改
		 */
		async rollback(msg?: string) {
			if (trx.isCompleted()) {
				return;
			}
			await trx.rollback(msg);
		},
		...dataWrap<T>(trx, tableName)
	};
}
