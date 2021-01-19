export interface OrderedListEntry<T>
{
  prev: OrderedListEntry<T>;
  next: OrderedListEntry<T>;
  value: T;
}

export type OrderedListIndex<T> = { [key: string]: OrderedListEntry<T> };

export class OrderedList<T>
{
  head: OrderedListEntry<T>;
  tail: OrderedListEntry<T>;
  index: OrderedListIndex<T>;

  constructor()
    {
      this.clear();
    }

  isempty(): boolean
    {
      return this.head == null;
    }

  clear(): void
    {
      this.head = null;
      this.tail = null;
      this.index = {};
    }

  insert(key: string, value: T): string
    {
      if (this.index[key] !== undefined)
        return `memsqs: send: message uid ${key} already exists`;

      let e: OrderedListEntry<T> = { prev: this.tail, next: null, value: value };
      if (this.tail)
        this.tail.next = e;
      this.tail = e;
      if (this.head === null)
        this.head = e;
      this.index[key] = e;
      return null;
    }

  remove(key: string): string
    {
      let e = this.index[key];
      if (e === undefined)
        return `memsqs: remove: message uid ${key} does not exist`;

      if (e === this.tail)
        this.tail = e.prev;
      else
        e.next.prev = e.prev;

      if (e === this.head)
        this.head = e.next;
      else
        e.prev.next = e.next;

      delete this.index[key];

      return null;
    }

  forEach(cb: (value: T) => boolean): void
    {
      for (let p = this.head; p && cb(p.value); p = p.next)
        continue;
    }
}
