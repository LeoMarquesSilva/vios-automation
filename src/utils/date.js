export function getLastWeekRange() {
  const now = new Date(); // hoje
  const day = now.getDay(); // 0=Domingo
  // Começo da semana passada (Segunda)
  const diffToMonday = (day + 6) % 7;
  const mondayLastWeek = new Date(now);
  mondayLastWeek.setDate(now.getDate() - diffToMonday - 7);
  const sundayLastWeek = new Date(mondayLastWeek);
  sundayLastWeek.setDate(mondayLastWeek.getDate() + 6);

  return {
    inicio: toBR(mondayLastWeek),
    fim: toBR(sundayLastWeek)
  };
}

function toBR(d) {
  return d.toLocaleDateString('pt-BR');
}
