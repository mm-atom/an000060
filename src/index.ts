import { Callback, QueryBuilder, Transaction } from '@mmstudio/an000049';
import wrapFilter from '@mmstudio/an000059';

export default function dataWrap<T extends {} = any>(table: () => QueryBuilder<T>) {
	type Data = Partial<T>;
	return {
		/**
		 * 新增
		 */
		insert(data: Data | Data[]) {
			return table().insert(data as any);
		},
		/**
		 * 修改
		 */
		update(data: Data, filter: Data) {
			return table().update(data as any).where(wrapFilter(filter));
		},
		/**
		 * 查询数据总条数
		 */
		async count(callback = ((q) => { return q; }) as Callback<T>) {
			const tb = table();
			const qb = (callback(tb) as typeof tb) || tb;
			const [{ size }] = await qb.count('*', { as: 'size' });
			return Number(size);
		},
		/**
		 * 查询列表
		 */
		async list(searchFields: Array<keyof T>, keywords: string, page: string | number, pagesize: string | number, filter = {} as Data, callback = ((q) => { return q.select('*'); }) as Callback<T>) {
			const size = Number(pagesize);
			const offset = (Number(page) - 1) * size;
			const tb = table();
			if (keywords) {
				tb.andWhere((builder) => {
					searchFields.forEach((field) => {
						builder.orWhere(field as string, 'like', `%${keywords}%`);
					});
				});
			}
			const query = wrapFilter(filter);
			const q = tb.where(query)
				.limit(size)
				.offset(offset);
			// eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
			const qb = (callback(q) as typeof q) || q;
			const data = await qb;
			const total = await this.count((qb) => {
				qb.where(query);
				if (!keywords) {
					return qb;
				}
				return qb.andWhere((builder) => {
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
			return table().where(wrapFilter(filter));
		},
		/**
		 * 查询一个
		 */
		first(filter = {} as Data) {
			return table().first('*').where(wrapFilter(filter));
		},
		/**
		 * 删除
		 */
		delete(filter = {} as Data) {
			return table().del().where(wrapFilter(filter));
		}
	};
}

// eslint-disable-next-line @typescript-eslint/ban-types
export function dataWrapTrx<T extends {} = any>(table: () => QueryBuilder<T>, trx: Transaction) {
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
		...dataWrap(table)
	};
}
