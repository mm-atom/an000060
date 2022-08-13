import { Knex } from 'knex';
import { Callback, QueryBuilder, Transaction } from '@mmstudio/an000049';
import wrapFilter from '@mmstudio/an000059';

export default function dataWrap<T extends {} = any>(db: Knex<any, unknown[]>, tableName: string) {
	function table() {
		return db<T>(tableName);
	}
	return {
		table,
		...tableWrap(table)
	};
}

export async function dataWrapTrx<T extends {} = any>(db: Knex<any, unknown[]>, tableName: string, trxOrTimeout?: Transaction | number) {
	type Data = Partial<T>;
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
		 * 新增或更新数据记录，支持单条或多条数据
		 * @example
		 * .insertOrUpdate({id: '001', name: 'foo'}, 'id');
		 * @example
		 * .insertOrUpdate([{id: '001', name: 'foo'},{id: '002', name: 'bar'}], 'id');
		 */
		async insertOrUpdate(data: Data | Data[], keyField: keyof Data, ...keyFields: (keyof Data)[]) {
			const allKeyFields = [keyField, ...keyFields].filter((kf) => {
				return Boolean(kf);
			});
			if (allKeyFields.length === 0) {
				throw new Error('没有有效的关键字');
			}
			if (Array.isArray(data)) {
				// 为了避免同时插入多条相同的数据，这里按顺序执行，避免异步带来的问题
				await data.reduce(async (ps, d) => {
					await ps;
					await this.insertOrUpdate(d, keyField, ...keyFields);
				}, Promise.resolve());
				return;
			}
			// Single Data
			const filter = allKeyFields.reduce((pre, cur) => {
				const val = data[cur];
				return {
					...pre,
					[cur]: val
				};
			}, {} as Data);
			const field = allKeyFields[0] as string;
			const row = await this.first(filter).select(field);
			if (row) {
				await this.insert(data);
			} else {
				await this.update(data, filter);
			}
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

export function tableWrap<T extends {} = any>(table: () => QueryBuilder<T>) {
	type Data = Partial<T>;
	return {
		/**
		 * 新增数据记录，支持单条或多条数据
		 * @example
		 * .insert({id: '001', name: 'foo'});
		 * @example
		 * .insert([{id: '001', name: 'foo'},{id: '002', name: 'bar'}]);
		 */
		async insert(data: Data | Data[]) {
			if (Array.isArray(data) && data.length === 0) {
				return;
			}
			await table().insert(data as any);
		},
		/**
		 * 修改
		 * @param data 要修改的数据，不修改的字段不用传
		 * @param filter 修改数据的条件，如`{a:123,b:'abc'}`对应的Sql为`where a=123 and b='abc'`
		 * @example
		 * 将`ID001`对应的数据的name字段值修改为`foo`
		 * .update({name:'foo'}, {id:'ID001'});
		 * @example
		 * 将`ID001`对应的数据的name字段值修改为空值
		 * .update({name: null}, {id:'ID001'});
		 * @example
		 * 如下代码将不会修改任何记录
		 * .update({name: undefined}, {id:'ID001'});
		 * @example
		 * 将`ID001`并且姓名为`foo`对应的数据的name字段值修改为`bar`
		 * .update({name: 'bar'}, {id:'ID001',name: 'foo'});
		 * @example
		 * 将姓名为`foo`并且ID不为`ID001`对应的数据的name字段值修改为`bar`
		 * .update({name: 'bar'}, {name: 'foo'})
		 * .andWhereNot({id:'ID001'});
		 * @example
		 * 将姓名为`foo`或`bar`并且ID不为`ID001`对应的数据的name字段值修改为`bar`
		 * .update({name: 'bar'}, {})
		 * .andWhere((qb)=>{
		 * 	return qb.where({name: 'foo'})
		 * 		.orWhere(name: 'bar');
		 * })
		 * .andWhereNot({id:'ID001'});
		 */
		update(data: Data, filter: Data) {
			return table().update(data as any).where(wrapFilter(filter));
		},
		/**
		 * 查询数据总条数
		 * @example
		 * 查询名称为`foo`的数据记录总数
		 * .count((qb) => {
		 * 	return qb.where({name:'foo'});
		 * });
		 * @example
		 * 查询名称为`foo`并且年龄大于`18`或者名称为`bar`的数据记录总数
		 * .count((qb) => {
		 * 	return qb.where((qb) => {
		 * 		return qb.where({ name: 'foo' })
		 * 			.andWhere('age', '>', 18);
		 * 	})
		 * 	.orWhere({ name: 'bar' });
		 * });
		 */
		async count(callback = ((q) => { return q; }) as Callback<T>) {
			const tb = table();
			const qb = (callback(tb) as typeof tb) || tb;
			const [{ size }] = await qb.count('*', { as: 'size' });
			return Number(size);
		},
		/**
		 * 查询列表
		 * @param searchFields	要使用关键字（第二个参数）匹配的字段
		 * @param keywords	要进行匹配的关键字
		 * @param page	当前页面，如果`pagesize`为0,该参数不生效
		 * @param pagesize	每一页取数的记录条数
		 * @param filter	要进行筛选的条件，如果使用复杂条件，可以传`{}`
		 * @param callback	排序或字段选择等**非Where条件**回调，这里如果传Where条件，数据总数查询结果将会是错的。
		 * @param whereCallback	复杂的Where条件
		 * @example
		 * 列出姓别为男，并且名字中包含`foo`的第一页数据，最多20条
		 * .list(['name'], 'foo', 1, 20, {sex: 1});
		 * @example
		 * 按名称正序排序列出姓别为男，并且名字中包含`foo`的第一页数据，只查询姓名字段，最多20条
		 * .list(['name'], 'foo', 1, 20, {sex: 1}, (qb) => {
		 * 	return qb.select('name')
		 * 	.orderBy('name', 'asc');
		 * });
		 * @example
		 * 列出姓名中包含`foo`的全部男性数据,这里页码参数不生效。
		 * .list(['name'], 'foo', 1, 0, {sex: 1});
		 * @example
		 * 按名称正序排序列出姓别为男或女（性别有`未知`的情况），并且名字中包含`foo`的第一页数据，只查询姓名字段，最多20条
		 * .list(['name'], 'foo', 1, 20, {}, (qb) => {
		 * 	return qb.select('name')
		 * 	.orderBy('name', 'asc');
		 * }， (qb) => {
		 * 	return qb.where({
		 * 		sex: 1
		 * })
		 * .orWhere({
		 * 		sex: 2
		 * })
		 * });
		 */
		async list(searchFields: Array<keyof T>, keywords: string, page: string | number, pagesize: string | number, filter = {} as Data, callback = ((q) => { return q.select('*'); }) as Callback<T>, whereCallback = ((q) => { return q; }) as Callback<T>) {
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
		 * @param filter 筛选条件
		 * @example
		 * 查询全部性别为男的数据
		 * .query({sex: 1});
		 * @example
		 * 查询全部性别为男或大于18岁女性(除`未知`外的)的数据
		 * .query({})
		 * .where({
		 * 	sex: 1
		 * })
		 * .orWhere((qb) => {
		 * 	return qb.where({
		 * 		sex: 2
		 * 	})
		 * 	.andWhere('age', '>', 18);
		 * });
		 * @example
		 * 查询全部数据
		 * .query();
		 */
		query(filter = {} as Data) {
			return table().where(wrapFilter(filter));
		},
		/**
		 * 查询一个
		 * @param filter 查询条件
		 * @example
		 * 查询年纪最小的`alian`
		 * .first({name: 'alian'})
		 * .orderBy('age', 'asc');
		 * @example
		 * 查询大于16中年纪最小的`alian`或`xiaowei`
		 * .first({})
		 * .where((qb) => {
		 * 	return qb.where({name: 'alian'})
		 * 		.orWhere({name: 'xiaowei'});
		 * })
		 * .andWhere('age', '>', 16)
		 * .orderBy('age', 'asc');
		 */
		first(filter = {} as Data) {
			return table().first('*').where(wrapFilter(filter));
		},
		/**
		 * 删除
		 * @param filter 筛选条件，正常情况下，条件值**不能为空**，也不能为`undefined`
		 * @example
		 * 删除全部男性数据
		 * .delete({sex: 1});
		 * @exmaple
		 * 删除所有叫`Davy`，且年龄大于18的人
		 * .delete({name: 'Davy'})
		 * .andWhere('age', '>', 18);
		 */
		delete(filter = {} as Data) {
			return table().del().where(wrapFilter(filter));
		}
	};
}
