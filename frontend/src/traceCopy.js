export function createTraceCopy(cycle) {
  if (cycle?.researchOnly) {
    return {
      thesisLabel: '2 - מסקנת מחקר ציבורי',
      intentLabel: '3 - כוונת מסחר',
      intentEmpty: 'לא הופקה כוונת מסחר; ה-LLM לא הופעל.',
      gateLabel: '4 - מסלול שערי סמכות (לא הופעל)',
      resultLabel: '6 - תוצאת המחקר',
      resultEmpty: 'המחקר הסתיים ללא קריאת LLM וללא פקודה.',
      memoryLabel: '7 - תיעוד פעילות',
    };
  }
  return {
    thesisLabel: '2 - תזה של LLM',
    intentLabel: '3 - כוונת מסחר',
    intentEmpty: 'עדיין לא הופקה פקודה.',
    gateLabel: '4 - מסלול שערי סמכות',
    resultLabel: '6 - תוצאת היומן',
    resultEmpty: 'לא נבנתה פקודה · הדסק החזיק.',
    memoryLabel: '7 - זיכרון שנכתב',
  };
}
