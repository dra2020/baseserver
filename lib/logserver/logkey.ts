export function dateToKey(d: Date): string
{
  let m = d.getUTCMonth() + 1;
  let day = d.getUTCDate();
  let yyyy = d.getFullYear();
  let sm = m < 10 ? `0${m}` : String(m);
  let sd = day < 10 ? `0${day}` : String(day);

  return `${yyyy}.${sm}.${sd}`;
}

export function dateToMonthKey(d: Date): string
{
  let m = d.getUTCMonth() + 1;
  let yyyy = d.getFullYear();
  let sm = m < 10 ? `0${m}` : String(m);

  return `${yyyy}.${sm}`;
}

export function dateToYearKey(d: Date): string
{
  return String(d.getFullYear());
}

export class LogKey
{
  kind: string;
  mode: string;
  date: string;
  dateKey: string;
  monthKey: string;
  yearKey: string;
  count: string;
  instance: string;
  ext: string;

  constructor()
  {
  }

  static root(): LogKey
  {
    let key = new LogKey();
    key.kind = 'Agg';
    return key;
  }

  static create(s: string): LogKey
  {
    let key = new LogKey();
    if (s == null || s == '') return null;
    let iExt = s.lastIndexOf('.');
    let ext = s.substring(iExt+1);
    s = s.substring(0, iExt);
    let a = s.split('_');
    if (a.length == 5)
    {
      key.kind = a[0]; key.mode = a[1]; key.date = a[2]; key.count = a[3]; key.instance = a[4]; key.ext = ext;
    }
    else if (a.length == 3)
    {
      key.kind = a[0]; key.mode = a[1]; key.date = a[2]; key.ext = ext;
    }
    else
    {
      key.kind = 'unknown'; key.dateKey = 'unknown'; key.instance = s;
    }

    if (key.kind === 'Log')
    {
      let d = new Date(key.date);
      key.dateKey = dateToKey(d);
      key.monthKey = dateToMonthKey(d);
      key.yearKey = dateToYearKey(d);
    }
    else if (key.date)
    {
      let keys = key.date.split('.');
      if (keys.length > 0)
        key.yearKey = keys[0];
      if (keys.length > 1)
        key.monthKey = `${keys[0]}.${keys[1]}`;
      if (keys.length > 2)
        key.dateKey = key.date;
    }
    return key;
  }

  yearClosed(): boolean { return (Number(this.yearKey) < Number(nowKey.yearKey)) }
  monthClosed(): boolean { return (Number(this.monthKey) < Number(nowKey.monthKey)) } 
  dateClosed(): boolean { return this.dateKey !== nowKey.dateKey }
  get closed(): boolean
  {
    if (this.kind === 'Log')
      return true;
    else if (this.kind === 'Agg')
    {
      if (this.dateKey)
        return this.dateClosed();
      else if (this.monthKey)
        return this.monthClosed();
      else
        return this.yearClosed();
    }
    else
      return true;
  }

  get yearID(): string { return `Agg_${this.mode}_${this.yearKey}.json` }
  get monthID(): string { return `Agg_${this.mode}_${this.monthKey}.json` }
  get dateID(): string { return `Agg_${this.mode}_${this.dateKey}.json` }

  get isYear(): boolean { return this.monthKey === undefined }
  get isMonth(): boolean { return this.monthKey !== undefined && this.dateKey === undefined }
  get isDate(): boolean { return this.kind === 'Agg' && this.dateKey !== undefined }
  get isLog(): boolean { return this.kind === 'Log' }
}

let nowKey: LogKey = new LogKey();
let nowDate = new Date();
nowKey.dateKey = dateToKey(nowDate);
nowKey.monthKey = dateToMonthKey(nowDate);
nowKey.yearKey = dateToYearKey(nowDate);

