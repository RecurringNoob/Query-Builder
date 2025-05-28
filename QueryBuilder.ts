

type SQLOperator = '=' | '!=' | '<>' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN' | 'NOT IN' | 'IS NULL' | 'IS NOT NULL';
type OrderDirection = 'ASC' | 'DESC';
type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';

interface WhereCondition<T> {
  column: keyof T;
  operator: SQLOperator;
  value?: unknown;
  logic?: 'AND' | 'OR';
}

interface JoinCondition {
  type: JoinType;
  table: string;
  on: string;
}

interface OrderByCondition<T> {
  column: keyof T;
  direction: OrderDirection;
}

class QueryBuilder<T extends Record<string | number | symbol, any>> {
  private table: string;
  private selectedColumns: (keyof T)[] | '*' = '*';
  private conditions: WhereCondition<T>[] = [];
  private joins: JoinCondition[] = [];
  private orderByConditions: OrderByCondition<T>[] = [];
  private limitValue?: number;
  private offsetValue?: number;
  private queryType: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' = 'SELECT';
  private insertData?: T | T[];
  private updateData?: Partial<T>;
  private groupByColumns: (keyof T)[] = [];
  private havingConditions: WhereCondition<T>[] = [];

  constructor(table: string) {
    this.table = table;
  }

  // Chain-safe method to create new instance for different query types
  static from<T extends Record<string | number | symbol, any>>(table: string): QueryBuilder<T> {
    return new QueryBuilder<T>(table);
  }

  // SELECT methods
  select<K extends keyof T>(...columns: K[]): QueryBuilder<T> {
    this.queryType = 'SELECT';
    this.selectedColumns = columns;
    return this;
  }

  selectAll(): QueryBuilder<T> {
    this.queryType = 'SELECT';
    this.selectedColumns = '*';
    return this;
  }

  selectRaw(expression: string): QueryBuilder<T> {
    this.queryType = 'SELECT';  
    this.selectedColumns = [expression as keyof T];
    return this;
  }

  // WHERE methods with improved type safety
  where<K extends keyof T>(column: K, operator: SQLOperator, value?: T[K]): QueryBuilder<T> {
    this.conditions.push({ column, operator, value, logic: 'AND' });
    return this;
  }

  orWhere<K extends keyof T>(column: K, operator: SQLOperator, value?: T[K]): QueryBuilder<T> {
    this.conditions.push({ column, operator, value, logic: 'OR' });
    return this;
  }

  whereIn<K extends keyof T>(column: K, values: T[K][]): QueryBuilder<T> {
    this.conditions.push({ column, operator: 'IN', value: values, logic: 'AND' });
    return this;
  }

  whereNull<K extends keyof T>(column: K): QueryBuilder<T> {
    this.conditions.push({ column, operator: 'IS NULL', logic: 'AND' });
    return this;
  }

  whereNotNull<K extends keyof T>(column: K): QueryBuilder<T> {
    this.conditions.push({ column, operator: 'IS NOT NULL', logic: 'AND' });
    return this;
  }

  // JOIN methods
  join(table: string, on: string): QueryBuilder<T> {
    this.joins.push({ type: 'INNER', table, on });
    return this;
  }

  leftJoin(table: string, on: string): QueryBuilder<T> {
    this.joins.push({ type: 'LEFT', table, on });
    return this;
  }

  rightJoin(table: string, on: string): QueryBuilder<T> {
    this.joins.push({ type: 'RIGHT', table, on });
    return this;
  }

  // ORDER BY methods
  orderBy<K extends keyof T>(column: K, direction: OrderDirection = 'ASC'): QueryBuilder<T> {
    this.orderByConditions.push({ column, direction });
    return this;
  }

  // GROUP BY and HAVING
  groupBy<K extends keyof T>(...columns: K[]): QueryBuilder<T> {
    this.groupByColumns = columns;
    return this;
  }

  having<K extends keyof T>(column: K, operator: SQLOperator, value?: T[K]): QueryBuilder<T> {
    this.havingConditions.push({ column, operator, value, logic: 'AND' });
    return this;
  }

  // LIMIT and OFFSET
  limit(count: number): QueryBuilder<T> {
    this.limitValue = count;
    return this;
  }

  offset(count: number): QueryBuilder<T> {
    this.offsetValue = count;
    return this;
  }

  // INSERT methods
  insert(data: T | T[]): QueryBuilder<T> {
    this.queryType = 'INSERT';
    this.insertData = data;
    return this;
  }

  // UPDATE methods
  update(data: Partial<T>): QueryBuilder<T> {
    this.queryType = 'UPDATE';
    this.updateData = data;
    return this;
  }

  // DELETE method
  delete(): QueryBuilder<T> {
    this.queryType = 'DELETE';
    return this;
  }

  // Helper methods for building query parts
  private buildWhereClause(conditions: WhereCondition<T>[]): string {
    if (conditions.length === 0) return '';

    return conditions.map((condition, index) => {
      const { column, operator, value, logic } = condition;
      const columnStr = String(column);
      
      let clause = '';
      
      // Add logic operator for non-first conditions
      if (index > 0) {
        clause += ` ${logic} `;
      }

      // Handle different operators
      switch (operator) {
        case 'IS NULL':
        case 'IS NOT NULL':
          clause += `${columnStr} ${operator}`;
          break;
        case 'IN':
        case 'NOT IN':
          if (Array.isArray(value)) {
            const values = value.map(v => this.sanitizeValue(v)).join(', ');
            clause += `${columnStr} ${operator} (${values})`;
          }
          break;
        default:
          clause += `${columnStr} ${operator} ${this.sanitizeValue(value)}`;
      }

      return clause;
    }).join('');
  }

  private sanitizeValue(value: unknown): string {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    if (typeof value === 'string') {
      // Basic SQL injection prevention - in production, use parameterized queries
      return `'${value.replace(/'/g, "''")}'`;
    }
    if (typeof value === 'boolean') {
      return value ? '1' : '0';
    }
    return String(value);
  }

  // Main build method
  build(): string {
    switch (this.queryType) {
      case 'SELECT':
        return this.buildSelectQuery();
      case 'INSERT':
        return this.buildInsertQuery();
      case 'UPDATE':
        return this.buildUpdateQuery();
      case 'DELETE':
        return this.buildDeleteQuery();
      default:
        throw new Error(`Unsupported query type: ${this.queryType}`);
    }
  }

  private buildSelectQuery(): string {
    const columns = this.selectedColumns === '*' ? '*' : 
      (this.selectedColumns as string[]).join(', ');
    
    let query = `SELECT ${columns} FROM ${this.table}`;

    // Add JOINs
    if (this.joins.length > 0) {
      const joinClauses = this.joins.map(join => 
        `${join.type} JOIN ${join.table} ON ${join.on}`
      ).join(' ');
      query += ` ${joinClauses}`;
    }

    // Add WHERE clause
    if (this.conditions.length > 0) {
      query += ` WHERE ${this.buildWhereClause(this.conditions)}`;
    }

    // Add GROUP BY
    if (this.groupByColumns.length > 0) {
      query += ` GROUP BY ${this.groupByColumns.join(', ')}`;
    }

    // Add HAVING
    if (this.havingConditions.length > 0) {
      query += ` HAVING ${this.buildWhereClause(this.havingConditions)}`;
    }

    // Add ORDER BY
    if (this.orderByConditions.length > 0) {
      const orderClauses = this.orderByConditions.map(order => 
        `${String(order.column)} ${order.direction}`
      ).join(', ');
      query += ` ORDER BY ${orderClauses}`;
    }

    // Add LIMIT and OFFSET
    if (this.limitValue !== undefined) {
      query += ` LIMIT ${this.limitValue}`;
    }
    if (this.offsetValue !== undefined) {
      query += ` OFFSET ${this.offsetValue}`;
    }

    return query;
  }

  private buildInsertQuery(): string {
    if (!this.insertData) {
      throw new Error('No data provided for INSERT query');
    }

    const dataArray = Array.isArray(this.insertData) ? this.insertData : [this.insertData];
    
    if (dataArray.length === 0) {
      throw new Error('Insert data array is empty');
    }

    const keys = Object.keys(dataArray[0]) as (keyof T)[];
    const columns = keys.join(', ');

    if (dataArray.length === 1) {
      // Single insert
      const values = keys.map(key => this.sanitizeValue(dataArray[0][key])).join(', ');
      return `INSERT INTO ${this.table} (${columns}) VALUES (${values})`;
    } else {
      // Batch insert
      const valueRows = dataArray.map(row => 
        `(${keys.map(key => this.sanitizeValue(row[key])).join(', ')})`
      ).join(', ');
      return `INSERT INTO ${this.table} (${columns}) VALUES ${valueRows}`;
    }
  }

  private buildUpdateQuery(): string {
    if (!this.updateData || Object.keys(this.updateData).length === 0) {
      throw new Error('No data provided for UPDATE query');
    }

    const updates = Object.entries(this.updateData)
      .filter(([, value]) => value !== undefined)
      .map(([key, value]) => `${key} = ${this.sanitizeValue(value)}`)
      .join(', ');

    let query = `UPDATE ${this.table} SET ${updates}`;

    if (this.conditions.length > 0) {
      query += ` WHERE ${this.buildWhereClause(this.conditions)}`;
    }

    return query;
  }

  private buildDeleteQuery(): string {
    let query = `DELETE FROM ${this.table}`;

    if (this.conditions.length > 0) {
      query += ` WHERE ${this.buildWhereClause(this.conditions)}`;
    }

    return query;
  }

  // Utility method to get parameter count for prepared statements
  getParameterCount(): number {
    return this.conditions.reduce((count, condition) => {
      if (condition.operator === 'IN' || condition.operator === 'NOT IN') {
        return count + (Array.isArray(condition.value) ? condition.value.length : 1);
      }
      if (condition.operator !== 'IS NULL' && condition.operator !== 'IS NOT NULL') {
        return count + 1;
      }
      return count;
    }, 0);
  }

  // Clone method for reusability
  clone(): QueryBuilder<T> {
    const cloned = new QueryBuilder<T>(this.table);
    cloned.selectedColumns = [...(this.selectedColumns === '*' ? ['*'] : this.selectedColumns)] as any;
    cloned.conditions = [...this.conditions];
    cloned.joins = [...this.joins];
    cloned.orderByConditions = [...this.orderByConditions];
    cloned.limitValue = this.limitValue;
    cloned.offsetValue = this.offsetValue;
    cloned.queryType = this.queryType;
    cloned.groupByColumns = [...this.groupByColumns];
    cloned.havingConditions = [...this.havingConditions];
    return cloned;
  }
}

// Example usage with improved interface
interface User {
  id: number;
  name: string;
  email: string;
  age?: number;
  created_at: Date;
  is_active: boolean;
}

// Example queries demonstrating enhanced functionality
const userBuilder = QueryBuilder.from<User>('users');

// Complex SELECT with joins, conditions, and ordering
const complexQuery = userBuilder
  .select('id', 'name', 'email')
  .leftJoin('posts', 'users.id = posts.user_id')
  .where('age', '>=', 18)
  .orWhere('is_active', '=', true)
  .whereNotNull('email')
  .orderBy('created_at', 'DESC')
  .limit(10)
  .offset(20)
  .build();

console.log('Complex SELECT:', complexQuery);

// Batch INSERT
const batchInsert = QueryBuilder.from<User>('users')
  .insert([
    { id: 1, name: 'John', email: 'john@example.com', age: 25, created_at: new Date(), is_active: true },
    { id: 2, name: 'Jane', email: 'jane@example.com', age: 30, created_at: new Date(), is_active: false }
  ])
  .build();

console.log('Batch INSERT:', batchInsert);

// UPDATE with conditions
const updateQuery = QueryBuilder.from<User>('users')
  .update({ is_active: false, age: 26 })
  .where('id', '=', 1)
  .build();

console.log('UPDATE:', updateQuery);

// DELETE with multiple conditions
const deleteQuery = QueryBuilder.from<User>('users')
  .delete()
  .where('is_active', '=', false)
  .where('age', '<', 18)
  .build();

console.log('DELETE:', deleteQuery);

// GROUP BY with HAVING
const groupQuery = QueryBuilder.from<User>('users')
  .selectRaw('COUNT(*) as user_count, age')
  .groupBy('age')
  .having('age' as keyof User, '>', 21)
  .orderBy('age', 'ASC')
  .build();

console.log('GROUP BY with HAVING:', groupQuery);

export { QueryBuilder, type SQLOperator, type OrderDirection, type JoinType };